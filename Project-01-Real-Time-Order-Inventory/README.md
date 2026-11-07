# Project 1 — Real-Time Order and Inventory Pipeline

**Tech stack from resume:** Apache Kafka, Python, Docker, Kubernetes, AWS S3  
**Goal:** Explain this project simply and confidently in a Data Engineering interview.

> **Important — evidence labels:** **[Resume]** means the information appears in the resume. **[Calculated]** means it was calculated from resume numbers using stated assumptions. **[Proposed]** means a realistic design or example to implement and verify; it is **not** confirmed to have been built. Never describe proposed features, unverified performance, or simulated benchmarks as proven professional work.

## 1. Problem Statement

### Simple explanation

Imagine an e-commerce company where customers place orders throughout the day. Every time someone buys an item, two things happen: the **order** status changes and the **inventory** (remaining stock) may change.

The operations team needs to know which orders are new, cancelled, or completed, and which products are low in stock. Earlier, the reporting system refreshed in a **four-hour batch cycle** **[Resume]**. As a result, the reports could be outdated between refreshes.

**The problem we wanted to solve:** make new order and stock information available much faster without counting duplicate events.

### Interview language

> “The problem was delayed visibility into orders and inventory. Our earlier reports were based on a four-hour batch cycle. We wanted an event-driven pipeline so that order and stock changes could be processed as they arrived. We also needed to catch duplicate records and keep historical data for analysis.”

**Business requirements:**
- Receive order and inventory events continuously.
- Check that incoming records are valid.
- Avoid counting the same event twice.
- Store event history for auditing and later analysis.
- Provide low-latency processed data for operational reporting.

## 2. Technologies Used

| Technology | Simple purpose | Why it fits |
|---|---|---|
| **Apache Kafka** | A place where services publish events and consumers read them | Keeps producers and consumers separate; supports buffering and replay |
| **Python** | Reads events and applies validation, deduplication, and transformations | Easy to write custom processing logic |
| **Docker** | Packs the Python application and dependencies together | Runs the same way in different environments |
| **Kubernetes** | Runs and restarts containers | Helps operate and deploy consumers |
| **AWS S3** | Stores processed event history | Durable, scalable object storage |
| **JSON** **[Proposed]** | Message format for events | Easy to inspect and exchange |
| **Parquet** **[Proposed]** | Storage format for processed analytical files | Compact and efficient for analytics |

**Resume support:** Kafka, Docker, Kubernetes, and S3 are named for the project, and the resume describes an event processor. Python is included in the resume skills; the precise implementation details are not stated.

## 3. Engineering Architecture

### Proposed flow

```text
         Order Service                 Inventory Service
               |                               |
               | order change                  | stock change
               v                               v
      Kafka: order-events             Kafka: inventory-events
               \                               /
                \                             /
                 v                           v
                Python Kafka Consumer / Processor
              (Docker container on Kubernetes)
                           |
                   1. Validate events
                   2. Check duplicates
                   3. Transform records
                           |
                 +---------+---------+
                 |                   |
                 v                   v
            AWS S3 archive     Reporting destination
              [Resume]            [To define]
```

> **Architecture gap:** The resume says the events became visible to operations teams, but it does **not** name the database, dashboard, API, or other system used to serve the processed data. We should choose and build a reporting destination before claiming a specific end-to-end reporting design.

### Step-by-step example

**Step A — An order happens.** A customer orders two shirts. The order application publishes an event such as:

```json
{
  "event_id": "evt-1001",
  "event_type": "ORDER_CREATED",
  "order_id": "ORD-501",
  "product_id": "SHIRT-10",
  "quantity": 2,
  "event_timestamp": "2026-11-07T10:00:00Z"
}
```

**Step B — Kafka receives the event.** In our **proposed design**, order events go to `order-events` and stock changes go to `inventory-events`. Kafka stores the records for a configured retention period.

**Step C — Python reads and checks the event.** The consumer validates fields and data types. It uses the event ID to detect duplicates. It then prepares the event for downstream use.

**Step D — The consumer is deployed.** Docker packages the application. Kubernetes runs the container and restarts it when needed. A restart by itself does not stop duplicates or data loss; the offset and output handling must be designed carefully.

**Step E — The event is stored.** Processed event history is archived in S3 **[Resume]**. We can **propose** date-based partitions, for example:

```text
s3://order-inventory-history/
  event_type=ORDER_CREATED/year=2026/month=11/day=07/part-0001.parquet
```

**Step F — Reporting receives the data.** We must design an actual query/serving layer and measure how quickly it shows updates. S3 storage alone does not prove that operations users can see an event within 25 seconds.

## 4. Design Choices: Why We Chose Them and Their Impact

| Decision | Reason in simple words | Impact / trade-off |
|---|---|---|
| **Events instead of waiting for a batch** | Don't wait hours to process new changes | Much fresher data; continuous systems are harder to monitor |
| **Kafka between applications and processor** | Order system does not have to wait for the reporting system | Can buffer events and replay them; needs broker management |
| **Separate order and inventory topics** **[Proposed]** | Keep different event types organized | Easier independent consumers and retention; more configuration |
| **Partition key = order ID or product ID** **[Proposed]** | Keep related events ordered inside a partition | Preserves per-key order; not global order |
| **Persistent event-ID deduplication** **[Proposed]** | A restart can otherwise make the same event count twice | Better correctness; requires a reliable state store or idempotent sink |
| **Docker and Kubernetes** | Standard deployment and container restart | Easier operations; may be more complex than necessary for the average volume |
| **AWS S3 historical archive** | Keep event records for later investigation | Durable and cost-effective; not a real-time dashboard by itself |

### A useful interview nuance

**140,000 events/day** is about **1.62 events/second on average**. This is not, by itself, a high event rate for Kafka. A better reason to choose Kafka is **decoupling, buffering bursts, replay, and independent downstream consumers**—not “our average traffic was extremely high.” We still need peak-rate measurements to justify the final sizing.

## 5. Results

### Results explicitly mentioned in the resume

| Metric | Value | Status |
|---|---:|---|
| Daily incoming events | ~140,000 | **[Resume]** |
| Reported pipeline latency | 25 seconds | **[Resume]**, measurement definition unknown |
| Earlier reporting batch cycle | 4 hours | **[Resume]** |
| Duplicate rate | 3% | **[Resume]** |
| Duplicate events in 30 days | ~126,000 | **[Resume]** and **[Calculated]** |

### Clear calculations using resume numbers

**Assumption for this simulation:** There are exactly 140,000 incoming events each day for 30 days, and exactly 3% of them are duplicates. We do not assume any other invalid events.

```text
Total events in 30 days = 140,000 × 30 = 4,200,000
Duplicate events         = 4,200,000 × 3% = 126,000
Non-duplicate events     = 4,200,000 − 126,000 = 4,074,000
Non-duplicate events/day = 140,000 × 97% = 135,800
Average incoming rate    = 140,000 / 86,400 ≈ 1.62 events/sec
```

**Comparing reported durations:** four hours = 14,400 seconds, so `14,400 / 25 = 576`. **Do not say the system is proven 576× faster.** A batch *schedule interval* and an event *processing latency* are different measures. We should compare the same end-to-end metric before claiming a speedup.

### Interview language

> “According to the project figures, the system handled about 140,000 order and inventory events each day with 25-second reported latency. We observed a 3% duplicate rate. Over 30 days, that means roughly 126,000 duplicate events out of 4.2 million incoming events, assuming a steady daily rate. The earlier reporting system refreshed on a four-hour batch cycle. I would validate the exact latency definition and performance comparison with the pipeline logs and benchmarks.”

## 6. Evaluation: How Do We Know It Worked?

The first question is not “Did the pipeline run?” but “**Did it produce correct results quickly and reliably?**”

### A. Check latency

Record a start timestamp when an event is created and an end timestamp when it is available in the reporting destination. Compute the difference.

```text
End-to-end latency = time visible to reporting − event creation time
```

Measure **P50**, **P95**, and **P99** (the times within which 50%, 95%, and 99% of events are available). The resume's “25 seconds” does **not** tell us which percentile it represents.

**Optional example only — simulated, not measured:** P50 = 9 seconds, P95 = 25 seconds, P99 = 42 seconds. These numbers illustrate how an evaluation table could look; they are not established project results.

### B. Check accuracy

Count events at each step. If all messages are either accepted, marked duplicate, or rejected:

```text
Received = Accepted + Duplicate + Rejected
```

For the simplified 30-day simulation with no invalid events:

```text
4,200,000 = 4,074,000 + 126,000 + 0
```

This is a calculation, **not** proof that no records were lost. We should also reconcile event IDs between the producer, Kafka, and destination.

### C. Check reliability

Test these situations:
- Consumer stops and starts again.
- Kafka delivers the same event a second time.
- Storage is temporarily unavailable.
- An event is missing a required field.
- Events arrive out of order.
- Traffic briefly becomes much higher than average.

For each test, verify recovery, correct counts, and no silent loss of events.

### D. Check throughput and lag

**Throughput** = how many messages we process per second. **Consumer lag** = how many messages are waiting to be read. If lag keeps growing, consumers may not be keeping up.

**Proposed load test:** send 10, 50, and 100 events per second in separate tests. Record P95 latency, lag, CPU/memory, and failure counts. These rates are **test targets**, not actual production peaks.

## 7. Guardrails (Safety Checks)

| Risk | Guardrail we should implement | Why it matters |
|---|---|---|
| Bad message | Validate fields and data types | Prevents incorrect downstream data |
| Duplicate message | Persistent event-ID check / idempotent write | Prevents double counting |
| Temporary failure | Retry with limits and backoff | Avoids losing data during brief outages |
| Repeatedly failing message | Dead-letter Kafka topic | Lets us investigate without blocking all processing |
| Consumer crashes | Commit Kafka offset **after** successful durable output | Reduces risk of missing records |
| Reprocessing same message | Idempotent destination or coordinated transaction | Prevents duplicate business effects |
| Unexpected delay | Alert on consumer lag and P95 latency | Detects slow processing early |
| Sensitive data | Minimum necessary fields, access controls, encryption | Limits data exposure |
| Large event history | Retention and lifecycle policies | Controls costs |
| Stock updates arrive out of order | Per-product sequence/version rules | Prevents an older update from replacing a newer one |

**Important clarification:** Kafka replay plus a Python set of IDs stored only in memory does **not** guarantee duplicate-free results. We must store deduplication state durably or make the final write idempotent. Likewise, committing Kafka offsets does not automatically make writes to S3 “exactly once.”

## 8. Interview Follow-Up Questions with Simple Answers

### Q1. Why Kafka?
**Answer:** Kafka helps applications send events without needing to know when reporting consumers will process them. It stores events for a period, lets consumers read them separately, and helps when traffic arrives in bursts. For very small systems, a simpler queue could be enough.

### Q2. What is a producer and a consumer?
**Answer:** A **producer** sends a message to Kafka. A **consumer** reads that message and processes it. In our proposed project, order and inventory services are producers; the Python processor is a consumer.

### Q3. What is a Kafka topic?
**Answer:** A topic is a named stream of messages. For example, `order-events` contains changes related to orders.

### Q4. What is a partition?
**Answer:** Kafka divides a topic into partitions. A partition stores messages in order. With a suitable message key, events for one order can be kept in the same partition. Kafka does not guarantee one global order across all partitions.

### Q5. Why separate order and inventory topics?
**Answer:** They are different business events. Separate topics make it easier to manage consumers, scaling, and retention independently. This is a **proposed** design choice, not a confirmed resume detail.

### Q6. What is a consumer group?
**Answer:** Consumers in a group share the topic partitions. Each partition is assigned to at most one active consumer in the group at a time. This lets us divide processing work.

### Q7. How did you handle duplicates?
**Answer:** We need a unique event ID and a **durable** way to record events already applied. If the same ID arrives again, we skip applying its business effect. The resume says duplicates were identified, but the exact storage and algorithm still need verification.

### Q8. What if a consumer crashes after writing data but before committing its Kafka offset?
**Answer:** Kafka may send that event again after restart. Therefore the destination must support idempotent writes, or we need a coordinated transaction strategy, so the same business event is not applied twice.

### Q9. What is consumer lag?
**Answer:** Lag is the difference between the latest offset available in Kafka and the offset a consumer has processed or committed, depending on the measurement. Growing lag means the consumer is falling behind.

### Q10. How would you prove 25-second latency?
**Answer:** I would collect an event creation timestamp and the time the corresponding event becomes available in the reporting system, then calculate the latency distribution. We need to check whether the resume's 25 seconds is an average, P95, or another measurement.

### Q11. Why Docker?
**Answer:** Docker packages the Python code and libraries into one image, making it easier to deploy the same application consistently.

### Q12. Why Kubernetes?
**Answer:** Kubernetes runs containers, restarts failed ones, and helps roll out new versions. However, at roughly 1.62 events/sec on average, we should justify Kubernetes based on operations and environment requirements rather than traffic volume alone.

### Q13. Why S3?
**Answer:** S3 is a durable place to keep the event history for future analysis, audits, and reprocessing. But we still need a reporting or serving layer for a live operational dashboard.

### Q14. Why choose Parquet instead of JSON on S3?
**Answer:** JSON is easy to read and debug. Parquet is usually smaller and better for analytical queries over selected columns. Parquet is a **proposed** improvement, not something proven by the resume.

### Q15. How would you handle messages that are out of order?
**Answer:** Use sensible partition keys and include event sequence numbers or versions where required. For inventory snapshots, reject older versions instead of overwriting newer stock values.

### Q16. What would you monitor?
**Answer:** Consumer lag, message count, processing errors, duplicate rate, end-to-end latency, retry count, dead-letter messages, and storage-write failures.

### Q17. Why not only use a batch job?
**Answer:** Batch is simpler and often cheaper. But if the team needs changes much sooner than a four-hour schedule allows, streaming helps improve freshness. Whether it is worth the extra complexity depends on the business requirement.

### Q18. How many partitions or consumer replicas did you use?
**Answer:** The resume does not specify those values. We should set them after measuring expected peak volume, processing time per message, and ordering needs. Do not make up an actual production setting.

### Q19. What was your personal contribution?
**Answer:** Describe only the components you actually built, configured, debugged, tested, or supported. We still need to document this separately.

### Q20. What would you improve next?
**Answer:** Implement a clear reporting destination, persistent deduplication, dead-letter handling, automated tests, end-to-end tracing, and repeatable load testing. Then capture measured P95 latency and recovery behavior.

## Short 2-Minute Interview Script

> “This project was about making order and inventory updates available faster for an e-commerce use case. Earlier, reporting was refreshed every four hours, so the operations team could be working with old information.
>
> The solution was an event-driven pipeline using Kafka for incoming order and stock events, Python for validation and deduplication, Docker and Kubernetes for running the processor, and AWS S3 to keep historical event data.
>
> The main flow was simple: source applications published events, Kafka received them, a Python consumer checked and processed them, and valid records were stored for historical use and downstream reporting.
>
> The key design decision was using Kafka to separate event generation from processing. That also gave us a way to buffer and replay events. Deduplication was important because about 3% of the events were duplicates.
>
> The resume figures are around 140,000 events per day and 25-second reported latency, compared with the previous four-hour batch reporting cycle. Assuming 30 days at that volume, a 3% duplicate rate means about 126,000 duplicate events.
>
> To evaluate the design properly, I would measure end-to-end reporting latency, Kafka consumer lag, record counts, failure recovery, and duplicate handling. The important reliability checks are validation, retry and dead-letter handling, persistent deduplication, and safe offset commits.”

**Before using this as a description of completed work:** Replace proposed steps with what you personally implemented, and verify the metrics using code, logs, or a repeatable demo. This guide is a learning and project-building document, not evidence of unverified employment experience.

## Verification Checklist

- [ ] Implement or locate the actual Kafka producer and consumer code.
- [ ] Confirm the topic names, partition keys, counts, and consumer-group behavior.
- [ ] Specify the durable deduplication method and verify replay handling.
- [ ] Specify the real reporting destination and when it receives events.
- [ ] Define what “25 seconds” measures and collect P50/P95/P99.
- [ ] Demonstrate 140,000 events/day or an equivalent repeatable workload.
- [ ] Check duplicate counts against total input and accepted output.
- [ ] Run failure/restart tests and record evidence.
- [ ] Document the specific work you personally completed.