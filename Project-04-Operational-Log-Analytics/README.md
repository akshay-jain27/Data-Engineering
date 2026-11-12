# Project 4 — Operational Log Analytics (V3)

**Company context:** Cognizant Technology Solutions  
**Resume technologies:** Python, SQL  
**Illustrative reference implementation:** Python 3, PostgreSQL 14+, JSON logs, Linux, Cron  
**Processing style:** Incremental five-minute micro-batches  
**Purpose:** Service-level error and latency analysis to support production incident investigations.

> **Evidence note / NDA boundary:** The resume establishes Python/SQL log processing, validation, deduplication, event-time service-level error and latency aggregations, and incident investigation support. The precise platform configuration, log volumes, database, scheduling, retention policies, and capacity targets below are **fictional but technically realistic reference assumptions**, not verified Cognizant deployment details or measured achievements. Discuss the implementation as a reference design unless it matches actual personal experience. Do not disclose proprietary implementation details.

## 1. Problem Statement

Production applications generate logs independently across services such as orders, payments, inventory, authentication, shipping, and notifications. When an incident occurs, engineers can spend substantial time examining unrelated records from many services.

**Challenges**
- Logs are scattered across services.
- Manual incident searches make it hard to isolate the affected service and time range.
- Without normalized metrics, recurring error and response-time patterns are harder to compare.

**Goal:** Ingest operational logs, validate and standardize them, remove duplicates, calculate error and latency metrics per service and event-time window, and retain enough detail to drill into the root cause.

**Interview wording:** "The operational team needed a consistent service-level view of errors and latency. The Python and SQL-based pipeline validated and deduplicated application logs, then produced service-and-time-window aggregates so engineers could locate an issue before inspecting individual log entries."

## 2. Technologies Used

| Component | Technology | Purpose |
|---|---|---|
| Ingestion/transformation | Python 3 | Parse JSON, read incrementally, validate and normalize |
| Relational storage | PostgreSQL 14+ (assumed) | Structured event and metrics tables, transactional writes |
| Analytics | SQL | Error counts, rates, response-time aggregates, investigation queries |
| Source | Rotating JSON Lines log files (assumed) | Application diagnostic events |
| Host and scheduler | Linux and Cron (assumed) | Run a job every five minutes |
| Diagnostics | Python logging | Job status, errors, reconciliation |

**Why not Spark?** The reference volume is moderate and incremental batches are small. Python with batched database writes is a reasonable first design. Distributed processing would be evaluated if measured throughput or complexity required it.

**Why not a dedicated search platform?** This case study's primary deliverable is a normalized analytical dataset, not arbitrary full-text search. Specialized observability tools could complement the solution.

## 3. Engineering Architecture

### Reference workload (illustrative, not resume facts)

| Parameter | Proposed value |
|---|---:|
| Production services | 10 |
| Incoming application log events/day | ~500,000 |
| Processing schedule | Every 5 minutes (288 runs/day) |
| Average input records/run | ~1,736 |
| Synthetic peak-load test | 3× average (~5,200/run) |
| Normal-batch duration objective | Under 2 minutes, **not measured** |
| Aggregation granularity | 5-minute event-time windows |
| Late-event reconciliation | 48 hours |
| Detailed event retention | 30 days |
| Aggregated metrics retention | 180 days |

The figures do **not** have to equal the other resume projects' numbers: order/inventory business events (140,000/day), changed customer records (8% of 1.8M), sales transaction rows (~2M/day), and application diagnostics measure different things and can arise in different workloads.

### End-to-end data flow

```text
10 production application services
          |
   Rotating JSON Lines logs
          |
 Read-only shared log directory
          |
 Linux Cron every 5 minutes
          |
 Python incremental reader + source checkpoints
          |
 Parsing / validation / UTC normalization
       /                     \
 Quarantine invalid       Valid records
 records                        |
                          Event-ID deduplication
                                |
                         PostgreSQL clean_logs
                                |
                    SQL event-time aggregation
                                |
                    service_log_metrics table
                                |
               Service/time filters + detailed log drill-down
```

### Step 1 — Source events

Example application log (illustrative):

```json
{
  "event_id": "evt_10245",
  "timestamp": "2026-10-15T10:15:20Z",
  "service": "payment-service",
  "request_id": "req_78901",
  "log_level": "ERROR",
  "http_status": 504,
  "response_time_ms": 2400,
  "error_code": "GATEWAY_TIMEOUT"
}
```

Not every log represents an HTTP request: `http_status` and `response_time_ms` are optional. The latency denominator is the number of eligible records with a valid measurement, *not* the count of all log messages.

### Step 2 — Incremental file processing

The Linux scheduler invokes a Python process:

```cron
*/5 * * * * /usr/bin/python3 /opt/log_analytics/process_logs.py
```

For each source, maintain `source_file_id`, `source_file_path`, `last_committed_offset`, `last_successful_run`, and status. Detect file rotation by stable file identity; finish unread bytes of the old file and then advance to the new file. Leave incomplete trailing lines until the next run.

Use an execution lock to prevent overlapping runs. Advance checkpoints only when associated database writes commit successfully. Use batching instead of one database insert per record.

### Step 3 — Validate and standardize

- Require parseable JSON, event ID, timestamp, service name, and a recognized log level.
- Normalize timestamps to UTC and response times to milliseconds.
- Reject impossible negative latency; preserve nulls for non-request events.
- Quarantine malformed or invalid events with source and rejection reason.
- Normalize service names and classify log levels consistently.

### Step 4 — Deduplicate and persist

Deduplicate by `event_id` within the batch, then enforce uniqueness across batches using PostgreSQL.

```sql
INSERT INTO clean_logs (
  event_id, event_timestamp, service_name,
  log_level, response_time_ms
)
VALUES (
  :event_id, :event_timestamp, :service_name,
  :log_level, :response_time_ms
)
ON CONFLICT (event_id) DO NOTHING;
```

This is an abbreviated example: the full write also stores request ID, status, error code, and ingestion timestamp. Separate errors with different IDs remain distinct even if their messages match. Initially choose a non-partitioned `clean_logs` table with an `event_id` primary key and an index on `(service_name, event_timestamp)`. If adding date partitioning, address the PostgreSQL restriction on unique constraints across partitions (for example, with a separate event-ID registry), rather than assuming global uniqueness is automatically enforced.

### Step 5 — Aggregate by event time

Detailed table: `clean_logs(event_id, event_timestamp, service_name, request_id, log_level, http_status, response_time_ms, error_code, ingestion_timestamp)`.

Aggregated table: `service_log_metrics(service_name, window_start, total_events, error_count, error_rate, latency_sample_count, latency_sum_ms, avg_latency_ms, max_latency_ms, last_updated)`.

```sql
SELECT
    service_name,
    date_bin(
        '5 minutes',
        event_timestamp,
        TIMESTAMPTZ '2000-01-01 00:00:00+00'
    ) AS window_start,
    COUNT(*) AS total_events,
    COUNT(*) FILTER (WHERE log_level = 'ERROR') AS error_count,
    ROUND(
        COUNT(*) FILTER (WHERE log_level = 'ERROR') * 100.0
        / NULLIF(COUNT(*), 0), 2
    ) AS error_rate,
    COUNT(response_time_ms) AS latency_sample_count,
    SUM(response_time_ms) AS latency_sum_ms,
    ROUND(AVG(response_time_ms), 2) AS avg_latency_ms,
    MAX(response_time_ms) AS max_latency_ms
FROM clean_logs
WHERE event_timestamp >= :start_time
  AND event_timestamp < :end_time
GROUP BY service_name, window_start;
```

The error rate here means **ERROR-level log events as a percentage of valid log events**. It is not necessarily HTTP request failure rate. Latency measurements are calculated only from records that have eligible response times.

Use an upsert keyed by `(service_name, window_start)` to replace affected aggregates. For a late record, recompute that window from `clean_logs`; do not append counts on top of an already calculated aggregate.

### Step 6 — Handle delayed events

If an event happened at 10:15 but was ingested at 10:27, assign it to the 10:15–10:20 event-time window. Within an illustrative 48-hour reconciliation period, recompute affected windows. Older events enter a controlled backfill process rather than being silently discarded. Track both event time and ingestion time.

### Step 7 — Investigation query

```sql
SELECT service_name, window_start, total_events,
       error_count, error_rate, avg_latency_ms
FROM service_log_metrics
WHERE window_start >= :start_time
  AND window_start < :end_time
ORDER BY error_count DESC, window_start DESC;
```

An engineer identifies the service and problematic window, then filters the detailed table by that service and time range to inspect individual error records.

### Failure recovery and transaction boundary

For this reference design, bulk writes, aggregate upserts, and the input checkpoint should be coordinated so that a failure cannot acknowledge uncommitted input. A robust implementation can commit accepted events and checkpoint atomically and recompute affected aggregates idempotently in a separately tracked step. Do not claim file-system writes to quarantine files are magically transactional with PostgreSQL: use idempotent quarantine record IDs or a transactional rejection table, and reconcile external exports.

## 4. Reasons for the Design and Impact

| Choice | Why | Benefit / trade-off |
|---|---|---|
| Python | Flexible parsing and validation for moderate input | Simple development; limited scale vs distributed processing |
| PostgreSQL | Structured, transactional SQL workload | Consistent metrics; not a full-text observability engine |
| 5-minute batching | Frequent analysis, no subsecond requirement | Simpler recovery; not real-time alert latency |
| Event-time windows | Arrival may be delayed | Accurate incident timeline; requires late-data reconciliation |
| Stable event IDs | Files/jobs may replay records | Idempotent writes; require trustworthy IDs |
| Detail + aggregate layers | Trends and root-cause analysis need different granularity | Fast targeted analysis and full drill-down |
| Incremental offsets | Avoid reading historical files on every run | Lower repeated work; requires robust rotation and checkpoints |

A five-minute schedule does not guarantee five-minute freshness. Real freshness also depends on log availability, job duration, and late arrivals.

## 5. Results

### Resume-supported qualitative results

- Validated log events and removed duplicates.
- Produced service-level error counts and latency analysis by event-time window.
- Enabled engineers to narrow problems to an affected service and period before reading detailed log records.

### Illustrative capacity scenario — NOT achieved results

- 500,000 incoming log events/day across 10 services.
- 24 × 60 ÷ 5 = **288** scheduled runs/day.
- 500,000 ÷ 288 ≈ **1,736** average incoming events/run.
- Synthetic 3× load test ≈ **5,200** records/run.
- Normal-batch processing target **under 2 minutes**, subject to benchmarking.

There is **no supported project-specific duplicate rate, invalid-rate percentage, latency SLA, or measured incident-time reduction** in the resume. Do not repeat the earlier invented 2% duplicate rate or 66.7% investigation improvement as factual outcomes.

**Interview wording:** "The result was more structured operational visibility. Engineers could first use aggregated error and latency metrics to locate the affected service and time period and then inspect only relevant detailed records."

## 6. Evaluation of Results

1. **Reconciliation:** Incoming = invalid + duplicate + accepted unique, using mutually exclusive classifications. Separately record discarded, quarantined, and successfully persisted counts.
2. **Correctness:** Manually verify sample service/time windows against the detailed events table; test unique event IDs, repeated messages, null response times, and error classification.
3. **Late arrivals:** Insert controlled delayed records and verify the originally affected windows are recomputed.
4. **Recovery:** Interrupt a run before/after database commit; confirm replay does not lose or duplicate records and that checkpoints are correct.
5. **Performance:** Measure normal and 3× test batches, CPU, memory, batch duration, query latency, backlog, and source-to-analytics freshness. The target is not a measured benchmark.
6. **Operational usefulness:** Compare how quickly engineers locate an affected service and time range across a representative incident set; claim a percentage only when measured.

## 7. Guardrails

- Schema and field validation; quarantine invalid JSON with rejection reasons.
- Duplicate prevention using event IDs and a database constraint.
- Idempotent inserts, aggregate replacement, and transactional/checkpoint discipline.
- Late-event reconciliation with a controlled backfill process.
- UTC timestamp normalization; account for incomplete source lines and file rotation.
- Execution lock, retries, failure logging, and monitoring of backlogs and bad-record rates.
- Least-privilege read access to source logs and controlled access to the analytics database.
- Redaction or exclusion of secrets/PII from log messages.
- Illustrative retention: 30 days for detail and 180 days for aggregates, dependent on policy.

## 8. Follow-up Interview Questions and Suggested Answers

**Q1. Explain the pipeline.**  
Logs from production services are read incrementally by Python, validated, deduplicated, stored as structured detail records, and aggregated with SQL into service- and event-time metrics for incident analysis.

**Q2. What did you personally implement?**  
Explain only the actual ingestion, transformation, validation, SQL, or operational components you performed. The reference architecture is not evidence of personal ownership.

**Q3. Why Python instead of Spark?**  
At the illustrative average of ~1,736 events per five-minute run, incremental Python and batched SQL writes can be sufficient; Spark adds distributed infrastructure that may not be warranted.

**Q4. Why PostgreSQL?**  
It provides SQL aggregations, indexes, uniqueness enforcement, and transactional writes for a moderate structured-analytics workload.

**Q5. Why use Cron?**  
A simple periodic job requires little orchestration; choose Airflow for more complex dependencies, visibility, or multi-pipeline workflows.

**Q6. How do you prevent duplicates?**  
Deduplicate stable event IDs in memory and use a database unique constraint across batches. Identical error messages are not automatically duplicates.

**Q7. What is event time versus processing time?**  
Event time is when the event happened; processing time is when the pipeline handled it. Aggregate by event time to retain correct incident timelines.

**Q8. What about late logs?**  
Recompute the touched service/time windows within a defined reconciliation window and backfill older records through a controlled process.

**Q9. What happens if a run fails?**  
Do not advance the checkpoint for uncommitted input; retry safely using event-ID uniqueness and idempotent aggregate upserts.

**Q10. How do you calculate error rate?**  
ERROR-level event count divided by all eligible valid log events in the same service/time window. Make clear this is not necessarily HTTP 5xx request rate.

**Q11. How do you calculate latency?**  
Use only events with a valid response-time measurement; calculate average and maximum and track the eligible sample count.

**Q12. What's a difficult engineering edge case?**  
Late-arriving and replayed events can corrupt time-window metrics without event-time recomputation and idempotent writes.

**Q13. How do you optimize SQL?**  
Use filtering by time/service, suitable indexes, batched ingestion, pre-aggregation, and `EXPLAIN ANALYZE` on slow queries.

**Q14. How would you scale to 10M daily logs?**  
Measure bottlenecks first; consider parallelized ingestion, partitioned storage, and Kafka/Spark or specialized analytical systems if justified.

**Q15. How is this different from Real-Time Order and Inventory?**  
That project handles business events for operational visibility; this one handles application diagnostics for troubleshooting.

**Q16. How is this different from Sales Transformation?**  
Sales Transformation uses PySpark to prepare reporting-ready business data, while this uses Python/SQL for operational log analysis.

**Q17. Why not Elasticsearch or Splunk?**  
They are strong observability/search tools; this reference design emphasizes a normalized SQL dataset and repeatable metrics rather than arbitrary log search.

**Q18. How do you test correctness?**  
Reconcile counts, compare sample windows to detail records, test duplicate replays, null latencies, late events, and safe retries.

**Q19. What's the relationship between transaction rows and log events?**  
There is no one-to-one mapping. A transaction may generate multiple diagnostic events, or none within the log collection scope.

**Q20. Why is this Data Engineering?**  
It involves ingestion, normalization, data quality, deduplication, incremental ETL, storage design, idempotency, and analytical modeling.

## 60–90 Second Interview Pitch

"One of the projects involved Operational Log Analytics, where the objective was to improve visibility into errors and latency issues across production services. Logs were scattered across services and engineers needed a consistent way to narrow investigations. The Python and SQL-based pipeline validated and deduplicated incoming application log records, standardized them, and produced error and response-time metrics grouped by service and event-time window. This made it easier to identify which service and time period needed investigation before drilling down into individual records. The core engineering concepts were incremental ETL, validation, deduplication, event-time aggregation, and reliable processing."

## Resume Alignment and Disclosure Notes

**Resume-supported:** Python, SQL, validated logs, duplicate removal, error counts and latency by service/event-time window, service-level incident investigation.

**Assumptions for this educational reference design:** 10 services, JSON Lines, shared read-only source, Linux/Cron, PostgreSQL, 500k/day, five-minute windows, 48-hour reconciliation, 30/180-day retention, 3× synthetic peak scenario, and two-minute normal-batch target.

**Do not present as verified achievements:** Simulated event volumes, processing targets, invented duplicate percentages, invented incident-resolution percentages, or claimed ownership of infrastructure that was not personally implemented.

**Review status:** V3 approved for addition as a reference guide, while actual confidential production details remain undisclosed.
