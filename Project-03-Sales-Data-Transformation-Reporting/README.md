# Project 3 — Sales Data Transformation & Reporting Pipeline

**Tech Stack:** Python, PySpark, Apache Spark, Spark SQL, AWS S3, Apache Parquet  
**Project Type:** Batch Data Engineering Pipeline  
**Goal:** Combine sales transactions, product information, and store-level datasets into reliable reporting-ready data.

## Resume Metrics

| Metric | Value |
|---|---|
| Daily workload | Approximately 2 million sales transaction records |
| Existing processing time | 45 minutes |
| Target processing time | 18 minutes |
| Target runtime reduction | 60% |

> **Evidence convention:** **[Resume]** means explicitly supported by the resume; **[Calculated]** means derived from resume numbers; **[Proposed]** means an illustrative implementation, validation rule, architecture detail, or example that must be verified before describing it as actual production work. In particular, the 18-minute runtime is a **target**, not a verified achievement.

---

## 1. Problem Statement

### Business Problem

The organization processed large amounts of sales information from different business systems:

1. **Sales transactions:** transaction ID, product ID, store ID, quantity, timestamp and sales amount.
2. **Product information:** product ID, name, category and other attributes.
3. **Store information:** store ID, name, location and region.

These datasets had to be cleaned, combined and summarized to generate daily sales reports. Duplicate or inconsistent records could affect reporting accuracy, and large joins and aggregations made the process slow. The existing reporting process took approximately **45 minutes**. **[Resume]**

### Business Requirements

- Process approximately **2 million transaction records daily**.
- Consolidate sales, product and store datasets.
- Handle duplicates and inconsistent data.
- Produce reporting-ready sales metrics.
- Store the final data in **AWS S3** using partitioned **Parquet** files.
- Target **18 minutes** of processing time instead of 45 minutes.

### Interview Explanation

> One of the projects I worked on at Cognizant was a Sales Data Transformation and Reporting Pipeline. The business had transaction, product and store data from multiple sources. We needed to clean, join and aggregate this data to generate accurate daily sales reports. The earlier process was taking around 45 minutes. We developed a PySpark-based batch pipeline using Spark SQL, S3 and Parquet, designed for about 2 million transactions per day, with a target runtime of 18 minutes.

---

## 2. Technologies Used

| Technology | Purpose |
|---|---|
| Python | Pipeline application logic |
| Apache Spark | Distributed data processing |
| PySpark | Implementing batch transformations in Python |
| Spark SQL | Joins, filtering, deduplication and aggregation |
| AWS S3 | Source and transformed dataset storage |
| Apache Parquet | Compressed, columnar analytical files |
| Date partitioning | Limit scanning of irrelevant reporting dates |

**Why PySpark?** It supports distributed execution of transformations, joins and aggregations, and can scale when data volumes or workload complexity grow. Two million rows alone do not automatically require Spark: total bytes, available resources, SLAs and existing infrastructure matter.

**Why Spark SQL?** Relational operations are easier to express, maintain and optimize using SQL queries.

**Why S3 and Parquet?** S3 separates scalable storage from compute. Parquet supports compressed columnar storage and efficient access to selected columns.

---

## 3. Engineering Architecture of the Project

### High-Level Architecture **[Proposed]**

```text
                  DATA SOURCES
          +------------+-------------+
          |            |             |
     Sales Data    Product Data    Store Data
          |            |             |
          +------------+-------------+
                       |
                       v
                AWS S3 RAW LAYER
                       |
                       v
                PYSPARK BATCH JOB
                       |
          +------------+------------+
          |            |            |
     Validation     Cleaning    Deduplication
          |            |            |
          +------------+------------+
                       |
                       v
             SPARK SQL TRANSFORMATIONS
              /                   \
       Dataset Joins         Aggregations
              \                   /
                       |
                       v
               REPORTING-READY DATA
                       |
                       v
               PARTITIONED PARQUET
                       |
                       v
              AWS S3 CURATED LAYER
                       |
                       v
               DAILY SALES REPORTS
```

**Unconfirmed:** Actual ingestion method, Spark execution environment, scheduler, reporting tool and precise operational layout are not specified in the resume.

### Step 1 — Data Ingestion **[Proposed]**

Illustrative source records:

**Sales**

| transaction_id | product_id | store_id | quantity | sales_amount |
|---|---|---|---|---|
| T101 | P101 | S01 | 2 | 1000 |
| T102 | P102 | S02 | 1 | 500 |
| T103 | P101 | S01 | 3 | 1500 |

**Products**

| product_id | product_name | category |
|---|---|---|
| P101 | Shirt | Clothing |
| P102 | Shoes | Footwear |

**Stores**

| store_id | store_name | region |
|---|---|---|
| S01 | Store A | North |
| S02 | Store B | South |

A possible raw zone:

```text
s3://sales-pipeline/raw/
    sales/
    products/
    stores/
```

### Step 2 — Read Data with PySpark **[Proposed]**

```python
sales_df = spark.read.parquet("s3a://sales-pipeline/raw/sales/")
products_df = spark.read.parquet("s3a://sales-pipeline/raw/products/")
stores_df = spark.read.parquet("s3a://sales-pipeline/raw/stores/")
```

Spark DataFrames support optimized distributed transformations. Spark's Catalyst optimizer can optimize the query plan.

### Step 3 — Validate and Clean the Input **[Proposed]**

Validate schema, key fields, numeric data types and invalid values.

```python
from pyspark.sql.functions import col

valid_sales = sales_df.filter(
    col("transaction_id").isNotNull()
    & col("product_id").isNotNull()
    & col("store_id").isNotNull()
)
```

In production, invalid records should be recorded, quarantined or handled under explicit business rules rather than silently dropped.

### Step 4 — Deduplicate Sales Records

**[Resume]** Deduplication is part of the project's transformations.

```python
sales_clean = valid_sales.dropDuplicates(["transaction_id"])
```

This example assumes `transaction_id` uniquely identifies a record. If transactions contain multiple line items, a composite key such as `(transaction_id, line_item_id)` may be appropriate. Differing versions also require a deterministic rule to choose the correct record.

### Step 5 — Join Datasets in Spark SQL

**[Resume]** Joins combine transaction, product and store information.

```sql
SELECT
    s.transaction_id,
    s.transaction_date,
    s.product_id,
    s.store_id,
    s.quantity,
    s.sales_amount,
    p.product_name,
    p.category,
    st.store_name,
    st.region
FROM sales_clean s
LEFT JOIN products p ON s.product_id = p.product_id
LEFT JOIN stores st ON s.store_id = st.store_id;
```

**[Proposed]** Left joins preserve sales rows when dimension attributes are missing; the actual join type is unconfirmed. Product and store data must have a valid unique-key or historical versioning strategy so joins do not multiply sales rows.

### Step 6 — Aggregate Sales Metrics

**[Resume]** The transformation includes aggregations to build reporting-ready tables.

```sql
SELECT
    transaction_date,
    region,
    category,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT transaction_id) AS transaction_count
FROM enriched_sales
GROUP BY transaction_date, region, category;
```

The output supports daily sales, category revenue and regional sales analysis. The correct distinct-count grain must match how transactions and line items are modeled.

**[Proposed] Example output:**

| Date | Region | Category | Total Sales |
|---|---|---|---:|
| 2026-11-10 | North | Clothing | 850,000 |
| 2026-11-10 | South | Footwear | 620,000 |
| 2026-11-10 | West | Electronics | 950,000 |

These are illustrative amounts, not production measurements.

### Step 7 — Store Partitioned Parquet in S3

**[Resume]** The pipeline writes partitioned Parquet datasets to AWS S3.

```python
report_df.write \
    .mode("overwrite") \
    .partitionBy("transaction_date") \
    .parquet("s3a://sales-pipeline/curated/")
```

**Note:** This illustrates partitioning only. Production write logic must avoid overwriting unrelated historical partitions and should support safe publication and reruns.

```text
s3://sales-pipeline/curated/
    transaction_date=2026-11-08/
    transaction_date=2026-11-09/
    transaction_date=2026-11-10/
```

### Step 8 — Reporting Consumption **[Proposed]**

Prepared data can support daily store sales, regional trends, product-category revenue and overall transaction reporting. The exact reporting application is not stated in the resume.

---

## 4. Reasons Behind Choosing the Design and Its Impact

| Design choice | Reason | Expected impact / trade-off |
|---|---|---|
| Batch over streaming | Requirement was daily sales reporting | Simpler processing; reports update after each batch |
| PySpark | Distributed joins and aggregation, scalable architecture | Handles growing workloads; introduces cluster overhead |
| Spark SQL | Relational transformations fit SQL naturally | Maintainable queries and optimizer support |
| Parquet over CSV | Columnar compressed analytical format | Reduced unnecessary reads; less human-readable |
| Date partitioning | Reporting queries filter by date | Partition pruning; risk of too many small files |
| AWS S3 | Storage independent of compute | Scalable shared storage |
| Join and shuffle tuning **[Proposed]** | Large joins can dominate runtime | Potential lower shuffle/network/disk cost |

### Candidate Optimizations **[Proposed]**

- **Column pruning:** Select only required columns.
- **Early filtering:** Reduce rows before joins and aggregation.
- **Broadcast join:** Broadcast a sufficiently small product or store table.
- **Shuffle reduction:** Eliminate unnecessary repartitioning and investigate expensive stages.
- **Partition tuning:** Match partition layout to the actual volume and workload.
- **Parquet output:** Produce an efficient reporting layout.

These explain plausible optimizations; which ones were actually implemented and measured must still be verified.

---

## 5. Results

### Resume-Based Performance

| Metric | Baseline | Design target |
|---|---:|---:|
| Daily sales transaction volume | ~2,000,000 | ~2,000,000 |
| Processing runtime | 45 minutes | 18 minutes |
| Average processing throughput **[Calculated]** | ~741 records/second | ~1,852 records/second |
| Runtime reduction | — | 60% |

**[Calculated] Time saved per run:** 45 − 18 = **27 minutes**.

**[Calculated] Runtime reduction:** (45 − 18) ÷ 45 × 100 = **60%**.

**[Calculated] Time saved over 30 daily runs:** 27 × 30 = **810 minutes = 13.5 hours**.

**[Calculated] Throughput baseline:** 2,000,000 ÷ (45 × 60) ≈ **741 records/s**.

**[Calculated] Throughput at target:** 2,000,000 ÷ (18 × 60) ≈ **1,852 records/s**.

These throughput figures average the full run and assume comparable 2-million-record workloads. They are not measurements of Kafka/event throughput or proof that the target was achieved.

### Business Impact

The design aimed to produce reports sooner, make sales datasets more consistent, improve product/store visibility and support larger daily workloads.

### Interview Explanation

> Our pipeline was designed for about 2 million daily transactions. The baseline processing time was 45 minutes and we targeted 18 minutes — a 60% reduction. We used Spark SQL transformations and stored the reporting-ready outputs in partitioned Parquet files on AWS S3. The goal was faster processing while maintaining reliable sales reporting.

---

## 6. Evaluation of the Results

We should evaluate **performance** and **correctness**, not only runtime.

| Metric | Evaluation approach |
|---|---|
| Processing duration | Compare job start/end timestamps across equivalent runs |
| Data completeness | Reconcile accepted, duplicate and rejected input records |
| Revenue correctness | Compare trusted source and transformed totals at the correct grain |
| Join accuracy | Find unmatched keys, duplicate dimension keys and unexpected row multiplication |
| Spark performance | Inspect Spark UI stage times, shuffles, skew, retries, executor resources |
| Output quality | Validate expected partitions, schema, nulls and downstream readability |

### Processing Time

`Processing time = job end time − job start time`. Benchmark with comparable input data, job scope and infrastructure. The **18-minute value remains a target** until measured.

### Data Completeness **[Proposed]**

```text
Source records
    = accepted unique records
    + duplicate records removed
    + rejected records
```

This equality requires mutually exclusive categories and agreed accounting rules.

### Revenue Reconciliation **[Proposed]**

For example, if a trusted source total is ₹50,000,000, the curated total should reconcile to it, considering legitimate business adjustments. The amount is illustrative.

### Join Accuracy **[Proposed]**

A sales-to-dimension enrichment should not unexpectedly multiply sales records. Verify dimension key uniqueness, unmatched references, and revenue totals.

### Spark Analysis **[Proposed]**

Inspect expensive stages, long-running tasks, shuffle read/write, data skew and executor utilization. Apply targeted optimizations and rerun benchmarks.

### Interview Explanation

> I would evaluate the pipeline on both processing time and accuracy. I would review Spark UI for slow stages and shuffles, compare source and output counts, validate joins and duplicates, and reconcile sales amounts. Faster processing is useful only if the reporting output is correct.

---

## 7. Guardrails and Data Quality Checks

The following are **recommended safeguards [Proposed]**, not production controls confirmed by the resume.

1. **Schema validation:** Reject or quarantine incompatible field types and missing required columns.
2. **Duplicate detection:** Apply business-key-aware, deterministic deduplication.
3. **Null/invalid-value checks:** Validate transaction, product, store, date and financial fields according to defined rules.
4. **Data reconciliation:** Track accepted, rejected and duplicate rows and reconcile revenue totals.
5. **Idempotent reruns:** Reprocessing identical input must not duplicate reporting results.
6. **Late-arrival handling:** Reprocess affected business-date partitions through a defined lookback or correction process.
7. **Monitoring and alerts:** Track job failures, runtime, counts, missing inputs and unexpected delays.
8. **Safe publication:** Validate outputs before exposing them; avoid partially visible or accidental destructive writes.

### Example Failure Scenario **[Proposed]**

If an upstream daily delivery is incomplete:

1. Detect missing input using delivery checks.
2. Hold publication instead of producing a misleading report.
3. Notify the responsible operator.
4. Restore or re-ingest the missing source.
5. Rerun processing safely.
6. Validate output and publish only after checks pass.

---

## 8. Possible Interview Follow-Up Questions

### Q1. Why batch rather than streaming?

Daily sales reporting did not require second-by-second updates. Batch was simpler for consolidation and aggregation.

### Q2. Why PySpark for 2 million records?

Spark provides distributed joins and aggregations and supports growth. Record count alone does not justify Spark; bytes, complexity, available resources, SLAs and existing infrastructure matter.

### Q3. What was your role?

My resume describes developing the PySpark batch pipeline, implementing Spark SQL joins/deduplication/aggregations, and designing partitioned Parquet outputs on S3.

### Q4. Spark vs PySpark?

Spark is the distributed processing engine. PySpark is the Python API used to build Spark applications.

### Q5. Why Spark SQL?

The key transformations were relational. SQL is clear and can benefit from Spark's Catalyst query optimizer.

### Q6. What is a shuffle?

Data is redistributed between partitions, often for joins, aggregations and repartitioning. Network/disk I/O can make it expensive.

### Q7. What is a broadcast join?

Spark distributes a small input to executors to join with a larger dataset without shuffling both sides. It is suitable only when the broadcast input fits resource constraints.

### Q8. Why Parquet instead of CSV?

Parquet supports compressed columnar storage and efficient analytical queries over selected columns.

### Q9. Why partition by date?

Date-filtered reporting queries can prune irrelevant partitions. However, partition design must account for file counts and skew.

### Q10. How did you deduplicate transactions?

Deduplication is resume-confirmed; the exact key is not. An implementation must use a valid unique transaction or line-item key and deterministic rules for conflicting versions.

### Q11. How would you troubleshoot a slow Spark job?

Inspect Spark UI stages and execution plans, shuffles, skew, join selection, column selection, partition sizing and executor resources. Change the real bottleneck and benchmark again.

### Q12. What is data skew?

Some partitions contain much more data than others, leaving slow tasks. Depending on the root cause, use skew-aware optimizations, changed partitioning or carefully applied salting.

### Q13. What if a job fails halfway?

Spark retries eligible tasks, but application failures still require recovery. Fix the cause and rerun safely without duplicating previously published outputs.

### Q14. What about late-arriving transactions?

Identify affected dates, reprocess required partitions using a defined lookback window, and publish replacements safely.

### Q15. What is idempotency?

The same input and business logic should produce the same logical output after retries or reruns.

### Q16. How do you verify joins?

Compare row counts and business totals before and after enrichment. Find unmatched keys and dimension duplicates.

### Q17. Partitioning vs bucketing?

Partitioning organizes data by column values; bucketing distributes records into a fixed number of hash-based buckets. Their performance uses differ by engine and table implementation.

### Q18. How did you achieve 60% improvement?

The resume specifies a **target**, not a verified achievement. Potential techniques include pruning, join/shuffle tuning, partition design and Parquet output. Do not claim measured improvement until benchmark results are available.

### Q19. Why AWS S3?

S3 offers scalable storage independent of Spark compute and supports raw/curated dataset organization.

### Q20. What would you improve in a redesign?

Add automated data-quality reconciliation, evaluate incremental processing, improve observability, and assess table formats/transactional publication for safer updates.

---

## Short 2-Minute Interview Explanation

> One of the Data Engineering projects I worked on at Cognizant was a Sales Data Transformation and Reporting Pipeline.
>
> The main objective was to improve daily sales reporting. We had sales transactions, product information and store-level datasets coming from multiple business sources. These datasets needed to be cleaned, joined and aggregated before they could be used for reporting.
>
> The challenges were inconsistent data, duplicate records and processing time. The existing process was taking approximately 45 minutes.
>
> We developed a batch pipeline using PySpark and Spark SQL, with AWS S3 and Parquet for storage. My work involved PySpark processing, joining sales and reference datasets, deduplication and aggregating transaction data into reporting-ready tables.
>
> We used partitioned Parquet files in S3 to organize the processed results. The pipeline was designed for approximately 2 million transaction records daily, with a performance target of 18 minutes rather than 45 minutes — a 60% reduction.
>
> The project focused on distributed batch processing, data integration, transformations and efficient reporting datasets.

---

## Project Summary

| Area | Detail |
|---|---|
| Business domain | Sales analytics and reporting |
| Pipeline | Batch ETL / transformation |
| Processing | Apache Spark / PySpark |
| Transformations | Spark SQL joins, deduplication, aggregation |
| Storage | AWS S3 |
| Format | Partitioned Parquet |
| Daily volume | Approximately 2 million sales transactions |
| Baseline | 45 minutes |
| Target | 18 minutes |
| Target improvement | 60% |
| Intended benefit | Faster, more consistent daily sales reporting |

**Verify before presenting proposed details as historical facts:** Spark execution platform, scheduler, ingestion process, exact deduplication key, actual benchmark results, implemented data-quality guardrails and reporting system.
