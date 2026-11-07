# Multi-Source Customer Data ETL — Interview Preparation

> Reference architecture and simulated examples, NOT verified professional achievements. Replace assumptions with actual project facts before interviews.

## 1. Problem Statement
Customer information is spread across CRM PostgreSQL, SQL Server orders, support REST APIs, and marketing CSV files. Formats, identifiers, and update frequencies differ. Duplicate and missing records make customer reporting unreliable. Goal: produce a trusted daily customer view.

**Interview wording:** The business had customer information in different systems. The pipeline collected, cleaned, matched, and published the data in one place for reporting.

## 2. Technologies Used (illustrative)
| Tool | Role |
|---|---|
| Python | API and file extraction |
| PostgreSQL, SQL Server, REST API, CSV | Example source systems |
| Amazon S3 | Raw and curated storage |
| PySpark | Cleaning, joining, deduplication |
| Apache Airflow | Scheduling, retries, dependencies |
| Amazon Redshift | Analytics warehouse |
| SQL | Validation and reporting |
| CloudWatch, IAM, Secrets Manager | Monitoring and security |

## 3. Engineering Architecture
Source databases / API / CSV → Python extractors → S3 raw zone → PySpark transformations → S3 curated Parquet → Redshift staging and MERGE → customer analytics.

Airflow orchestrates every step and logs run IDs, row counts, timestamps, failures, and watermarks.

**Detailed flow:**
1. Extract changes from databases using CDC or a reliable updated timestamp. Paginate APIs and detect new CSV files.
2. Land immutable source copies in S3 with source name, ingestion date, and run ID.
3. Validate required fields, types, schema, and counts. Quarantine invalid records.
4. Standardize emails, phones, timestamps, and column names using PySpark.
5. Deduplicate within each source by primary key and most recent business timestamp.
6. Resolve cross-system identities using verified mappings; flag ambiguous matches rather than guessing.
7. Create curated customer, orders, and support datasets in Parquet.
8. Stage Redshift data, reconcile counts, then publish with transactional upserts.
9. Advance the incremental watermark only after successful publication.

**Example tables:** dim_customer(customer_sk, source_ids, email_normalized, updated_at); fact_orders(order_id, customer_sk, amount, order_time); fact_support_tickets(ticket_id, customer_sk, opened_at, status).

## 4. Design Choices and Impact
| Choice | Reason | Trade-off |
|---|---|---|
| S3 raw layer | Replay and audit source data | Additional storage |
| Incremental extraction | Avoid daily full scans | Must handle deletes and late updates |
| PySpark + Parquet | Distributed transformations and columnar storage | Overhead for small workloads |
| Airflow | Visible dependencies, retries, and alerts | Operational complexity |
| Redshift staging + MERGE | Controlled upserts and publication | Requires stable keys and transactions |
| Quarantine invalid rows | Preserve evidence while protecting reporting | Requires remediation process |

**Incremental example:** Read changed rows with a small overlap from the last committed watermark to a fixed extraction cutoff. Deduplicate by source primary key and latest update. Do not advance the watermark until the warehouse load succeeds. For deletions use CDC tombstones or explicit deletion markers.

## 5. Results — SIMULATED ONLY
| Metric | Hypothetical baseline | Hypothetical result |
|---|---:|---:|
| Daily rows | 2 million | 2 million |
| Runtime | 180 minutes | 55 minutes |
| Duplicate records needing attention | 8% | 0.5% |
| Reporting freshness | 24–48 hours | Within 2 hours of scheduled load |

The hypothetical runtime reduction is about 69.4%. None of these figures has been verified against a real project.

## 6. Evaluation of Results
Compare source, landed, accepted, quarantined, and published row counts. Check unique business keys, source-to-target reconciliation, customer match accuracy, freshness lag, success rates, rerun idempotency, processing cost, and performance at 2x and 5x volume. Establish actual baselines before reporting improvement.

## 7. Guardrails
- Idempotent ingestion and upserts: reruns cannot duplicate records.
- Quality gates: stop publication for missing critical keys or failed reconciliations.
- Security: encrypt PII, restrict IAM permissions, mask sensitive logs, and use Secrets Manager.
- Observability: log batch IDs, watermarks, counts, exceptions, and SLA alerts.
- Schema drift: detect changes before downstream breakage.
- Recovery: retry transient failures, quarantine malformed rows, and replay failed batches.
- Compliance: define retention, deletion, and access controls for customer data.
- Late arrivals and deletes: use overlap/CDC and process source deletion signals.

## 8. Likely Interview Questions
**Why S3 before Redshift?** It preserves raw data for replay and separates ingestion from processing.

**Why Spark instead of pandas?** Spark distributes large transformations; pandas can be simpler for smaller datasets.

**How did you deduplicate?** By source primary key and latest update, followed by carefully governed cross-source identity matching.

**What happens when a task fails?** Airflow retries temporary failures; watermarks advance only after success, so the batch can be rerun safely.

**What is incremental loading?** Reading only records changed since the previous successful load rather than scanning all history.

**How do you handle late data?** Overlap windows or CDC positions plus idempotent upserts.

**How do you handle API limits?** Pagination, checkpointing, retry-after handling, and exponential backoff.

**How do you ensure quality?** Required-field checks, uniqueness, schema checks, reconciliation, and publish-blocking rules.

**ETL vs ELT?** ETL transforms before the warehouse load; ELT transforms inside the warehouse. This example is primarily ETL.

**How do you measure success?** Runtime, freshness, accuracy, cost, reliability, and recovery time.

**What if a customer requests deletion?** Apply a governed deletion process across mapped identities and retained datasets, subject to retention rules.

**What would you improve?** Consider CDC, data contracts, lineage, and stronger observability.

## 60-Second Interview Explanation (adapt to real experience)
Customer data was spread across a CRM, an order system, support APIs, and CSV files. The reference design uses Python to ingest, S3 to retain raw data, PySpark to standardize and deduplicate, Airflow to orchestrate, and Redshift for reporting. Incremental extraction avoids full daily reads, and idempotent upserts make retries safe. Quality gates prevent incorrect data from reaching analytics. Performance and data-quality improvements must be measured and verified rather than assumed.

## Verification Checklist
Confirm real source systems, tools, daily volumes, scheduling, your actual responsibilities, production controls, and observed results before describing this as work you completed.