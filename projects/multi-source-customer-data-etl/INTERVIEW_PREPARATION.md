# Multi-Source Customer Data ETL — Resume-Aligned Interview Preparation

**Source:** Data_engineering_resume.pdf, second Data Engineering project under Cognizant.  
**Evidence labels:** **Resume fact** = explicitly stated in resume; **Derived** = arithmetic from resume values; **Illustrative** = possible design or answer, not confirmed implementation.

## 1. Problem Statement

**Resume fact:** Customer, order, and support records needed to be consolidated into a consistent dataset for reporting across business teams. The original customer data refresh took **60 minutes**.

**Simple explanation:** "The company had customer-related data coming from different systems. We needed to combine it into a consistent dataset for reporting, but processing the full source was slow."

**Illustrative context, not confirmed:** Different systems may have different formats, identifiers, and update frequencies. The resume does not identify specific databases, APIs, or file types.

## 2. Technologies Used

**Resume-confirmed project stack:**

| Technology | Role in this project |
|---|---|
| Python | Incremental ETL jobs |
| Docker | Package jobs for deployment |
| Kubernetes | Orchestrate execution of Python jobs |
| AWS S3 | Store partitioned datasets |
| Amazon Athena | Analyze data stored in S3 |

**Do not substitute:** PostgreSQL, SQL Server, Redshift, or PySpark as actual project technologies without verification. Airflow appears in the general skills section of the resume, but **not** in this project's technology list or bullets.

## 3. Engineering Architecture

```text
Customer records ─┐
Order records ─────┼──> Incremental Python ETL jobs
Support records ───┘           |
                               v
                      Docker containers
                               |
                               v
                    Kubernetes execution
                               |
                               v
                   Partitioned datasets in S3
                               |
                               v
                       Amazon Athena
                               |
                               v
                    Business team reporting
```

**Resume fact:** The jobs were incremental, ran on Kubernetes, used Docker for deployment, and produced partitioned S3 datasets for Athena analysis.

**Illustrative implementation sequence (verify before claiming):**
1. Identify a batch of records changed since the last successful refresh.
2. Read those records using Python.
3. Standardize and consolidate customer, order, and support fields.
4. Write the resulting dataset into appropriate S3 partitions.
5. Make the partitioned data queryable through Athena.
6. Track completion and rerun failed work safely.

The resume does **not** state the precise change-detection mechanism, scheduling service, data model, file format, partition key, or job failure handling. Those details should be confirmed.

## 4. Reasons for Design Choices and Their Impact

| Design | Why it makes sense | Resume support |
|---|---|---|
| Incremental processing | Avoid reprocessing unchanged source records | Confirmed; only 8% changed |
| Python jobs | Flexible data extraction and transformation | Confirmed |
| Docker | Consistent packaging and deployment | Confirmed |
| Kubernetes | Execute and manage containerized workloads | Confirmed |
| Partitioned S3 datasets | Reduce unnecessary scanning for suitable Athena queries | Confirmed use; exact partition strategy unspecified |
| Athena | Query datasets in S3 for analysis | Confirmed |

**Interview wording:** "The biggest optimization was incremental processing. Instead of processing all 1.8 million source rows every time, the refresh focused on the 8% that changed."

## 5. Results — From Resume, Not Simulated

| Metric | Value | Evidence |
|---|---:|---|
| Total source rows | 1,800,000 | Resume fact |
| Records changed per refresh | 8% | Resume fact |
| Records processed in incremental scope | 144,000 | Derived: 1,800,000 × 0.08 |
| Unchanged records avoided | 1,656,000 | Derived |
| Processing-volume reduction | 92% | Resume fact |
| Previous refresh runtime | 60 minutes | Resume fact |
| New refresh runtime | 12 minutes | Resume fact |
| Runtime reduction | 80% | Derived: (60−12)/60 |
| Runtime speedup | 5× | Derived |
| Time saved per daily run | 48 minutes | Derived |
| Time saved across 30 daily runs | 24 hours | Resume fact and derived: 48 × 30 = 1,440 minutes |

**Important distinction:** The **92% reduction** is in records processed, whereas the **80% reduction** is in elapsed refresh time. Do not confuse the two.

## 6. Evaluation of Results

**Supported outcome:** The resume reports a drop from 60 minutes to 12 minutes while narrowing the refresh to 8% of a 1.8-million-row source.

**What to validate in a real evaluation (not claimed in resume):**
- Compare source change counts with records ingested and published.
- Reconcile customer, order, and support totals.
- Measure Athena query latency and bytes scanned for partition-filtered queries.
- Test reruns for duplicates or missing updates.
- Verify whether the 12-minute refresh time is typical across multiple days.
- Check correctness for late-arriving changes and deletions.

## 7. Guardrails — Recommended, Not Confirmed

The resume does not list guardrail implementations. The following are interview discussion points, **not statements of completed work**:

1. Maintain a checkpoint or watermark and advance it only after successful processing.
2. Make writes idempotent so a retry does not duplicate customer data.
3. Validate required identifiers, data types, and row counts.
4. Detect late-arriving changes and source deletions.
5. Restrict access to customer data with IAM and encryption.
6. Log job failures and alert owners.
7. Avoid accidental S3 partition overwrites and orphaned output.
8. Validate Athena partition metadata and partition pruning.

## 8. Possible Interview Follow-up Questions

**Q1. What was the main business problem?**  
A: "Customer, order, and support records needed to be consolidated for reporting. The previous refresh took an hour."

**Q2. What exactly did you optimize?**  
A: "The incremental refresh focused on the 8% of rows that changed rather than processing the whole 1.8-million-row source."

**Q3. How many rows is 8% of 1.8 million?**  
A: "144,000 changed rows, leaving about 1,656,000 unchanged rows outside the incremental processing scope."

**Q4. Why did runtime improve by 80% rather than 92%?**  
A: "The 92% refers to reduced row-processing volume. The runtime dropped from 60 to 12 minutes, which is an 80% reduction. Fixed startup, I/O, and write overhead can prevent runtime from scaling exactly with row count." The overhead explanation is illustrative.

**Q5. Why use Docker?**  
A: "Docker packages the Python job and its dependencies into a consistent deployable image."

**Q6. Why Kubernetes?**  
A: "Kubernetes runs and manages the containerized jobs." The exact scheduling and retry configuration is not in the resume.

**Q7. Why S3 and Athena?**  
A: "S3 stores the datasets, and Athena lets teams query them. Partitioning can reduce scanned data when queries filter on partition keys."

**Q8. How did you identify changed records?**  
A: "The resume confirms incremental processing but not whether we used timestamps, CDC, hashes, or another method. I need to verify the actual approach before giving a specific answer."

**Q9. How did you handle duplicates?**  
A: "The resume does not describe the deduplication rules for this project. A typical approach uses source keys and deterministic upserts, but the actual implementation needs verification."

**Q10. How did you handle failed jobs?**  
A: "The resume does not document recovery behavior. A robust approach would keep checkpoints unchanged until successful completion and make reruns safe."

**Q11. How did you choose S3 partitions?**  
A: "The resume confirms partitioned datasets but does not specify partition keys. The appropriate choice depends on query filters and data distribution."

**Q12. How were 24 hours saved?**  
A: "Each run saved 48 minutes. Over 30 daily runs, that's 1,440 minutes, or 24 hours."

## 60-Second Interview Explanation — Resume-Aligned

"One project was Multi-Source Customer Data ETL. We consolidated customer, order, and support records into a consistent dataset for business reporting. The pipeline used incremental Python jobs deployed with Docker and run on Kubernetes. We stored partitioned datasets in AWS S3 so they could be analyzed through Amazon Athena. The key improvement was reducing each refresh to the records that had changed: around 8% of a 1.8-million-row source. That reduced the processing volume by 92%, and refresh time dropped from 60 minutes to 12 minutes. With one daily run, that represents about 24 hours saved over 30 days."

## Details to Confirm Before a Technical Interview

- What were the exact source systems?
- How were changed records detected?
- How did the Python jobs join or reconcile customer, order, and support data?
- What was the S3 file format and partition key?
- How were Kubernetes jobs triggered, monitored, and retried?
- How were Athena tables registered and refreshed?
- How were deletes, late updates, duplicate rows, and failed runs handled?
- Were the performance measurements observed in production or tests?

**Do not present illustrative answers or architecture choices as verified professional experience.**
