# **BigQuery Indirect Writes Sink**

## **What?**

Offer a new, cheaper Flink BigQuerySink implementation that writes data to BigQuery by:

1. Staging files on GCS  
2. Uploading those files to BigQuery via load jobs

In comparison, the existing Flink BigQuerySink implementation writes to BigQuery directly via the BigQuery Storage Write API.

## **Why?**

Writing via the Storage Write API can be expensive for high-volume workloads. The Storage Write charges [$0.025/GiB](https://cloud.google.com/bigquery/pricing?hl=en#bigquery-services-pricing) per byte ingested (after the first 2 TiB per month). 

In comparison, load jobs are **free** when used in conjunction with BigQuery’s (default) shared slot pool. The only costs for the indirect-write method are therefore ephemeral GCS storage costs of the staged GCS files. Note: The GCS bucket must be in the same region as the BigQuery dataset to avoid cross-region network charges.

There is potentially significant cost savings. Assuming Flink jobs write 1 PB of data to BigQuery daily:

| Method                            | Estimated Daily cost | Estimated Yearly cost |
|-----------------------------------|----------------------|-----------------------|
| Storage Write API                 | \~$25,000            | \~$9,125,000          |
| Indirect write                    | \~$0                 | \~$0                  |

## **Tradeoffs**

Indirect writes come with two notable tradeoffs:

* **Higher latency.** Load jobs can take multiple minutes whereas writing via the Storage Write API is in the seconds range.  
* **No guaranteed capacity.** Google does not guarantee there will always be available capacity in the shared slot pool. 

Indirect writes are therefore best suited to **batch** workloads where latency on the order of minutes is acceptable.

## **Architecture**

### **High Level Flow**

```
flowchart LR
    subgraph Flink Job
        A[Input Records<br/>RowData] --> B[FileSink's FileWriter<br/>per subtask]
        B -->|Parquet files| C[(GCS Staging<br/>Bucket)]
        B -->|checkpoint / end of input| D[FileSink's FileCommitter]
        D -->|CommittableMessage| E[BigQueryLoadJobOperator<br/>post-commit topology]
    end

    E -->|Load Job per<br/>complete checkpoint| F[(BigQuery<br/>Table)]
    E -->|cleanup| C
```

In batch mode, all committables arrive before close(). The function eagerly submits load jobs from flatMap() as each checkpoint completes. 

```
sequenceDiagram
    participant R as Input Records
    participant W as FileWriter (via FileSink)
    participant GCS as GCS (Parquet files)
    participant FC as FileCommitter (via FileSink)
    participant LJ as BigQueryLoadJobOperator
    participant BQ as BigQuery
    R ->> W: write(RowData)
    W ->> W: RowDataParquetWriterFactory writes RowData rows
    W ->> GCS: append to Parquet file
    Note over W: End of input reached
    W ->> W: prepareCommit()
    W -->> FC: FileSinkCommittable (pending files)
    FC ->> GCS: finalize .inprogress => final files
    FC ->> LJ: CommittableSummary + CommittableWithLineage
    Note over LJ: flatMap(): group files by checkpoint ID,<br/>track checkpoint completion
    Note over LJ: Checkpoint complete =><br/>submitCompleteCheckpoints() eagerly
    LJ ->> BQ: submit LoadJob (see Load Job Submission below)
    LJ ->> GCS: delete staged files (best-effort)
    Note over LJ: close() => no-op (already submitted)
```

### **Streaming Mode Flow**

In streaming mode, load jobs are submitted eagerly from flatMap() as each checkpoint completes (all summaries and committables received).

```
sequenceDiagram
    participant R as Input Records
    participant W as FileWriter (via FileSink)
    participant GCS as GCS (Parquet files)
    participant FC as FileCommitter (via FileSink)
    participant LJ as BigQueryLoadJobOperator
    participant BQ as BigQuery

    loop Per checkpoint
        R ->> W: write(RowData)
        W ->> GCS: append to Parquet file
        Note over W: Checkpoint barrier
        W ->> W: prepareCommit()
        W -->> FC: FileSinkCommittable (pending files)
        FC ->> GCS: finalize .inprogress => final files
        FC ->> LJ: CommittableSummary + CommittableWithLineage
        Note over LJ: flatMap(): group files by checkpoint ID
        Note over LJ: Checkpoint commit complete
        LJ ->> BQ: submit LoadJob per complete checkpoint commit
        LJ ->> GCS: delete staged files (best-effort)
    end
```

### **Load Job Submission**

```
flowchart TD
    A[Files from checkpoint N] --> B{files ≤ 10K<br/>AND ≤ 15 TB?}

B -->|Yes|C[Direct Load to Final Table]
C --> C1["LoadJob(finalTable, allUris)<br/>job ID: flink-bq-load_{hash}_cN_{uriHash}"]
C1 --> C2["Delete GCS files<br/>(best-effort)"]

B -->|No|D[Partition files into chunks]
D --> E["Partition 0: files[0..9999]"]
D --> F["Partition 1: files[10000..19999]"]
D --> G["..."]
D --> H["Partition 9: files[90000..99999]"]

E --> I["LoadJob => temp_p0<br/>WRITE_TRUNCATE"]
F --> J["LoadJob => temp_p1<br/>WRITE_TRUNCATE"]
G --> K["..."]
H --> L["LoadJob => temp_p9<br/>WRITE_TRUNCATE"]

I --> M["Atomic COPY Job<br/>[temp_p0..temp_p9] => finalTable<br/>WRITE_APPEND"]
J --> M
K --> M
L --> M

M --> N["Delete temp tables"]
N --> O["Delete GCS files<br/>(best-effort)"]
```

## **How Exactly-Once Works**

Exactly-once delivery relies on four mechanisms working together:

1. **Flink checkpointing** ; checkpoint barriers align all writers; writer state and committables are durably stored. On failure, Flink replays from the last successful checkpoint, producing the exact same set of files.  
2. **Deterministic job IDs** ; job IDs embed a UUID that persists across internal restarts, plus the checkpoint ID. The UUID is unique per operator instance, so multiple indirect sinks in the same Flink job produce distinct job IDs. On replay, the same operator instance produces the same job ID. BigQuery job IDs are globally unique and immutable: once a job ID completes, resubmitting the same ID is a no-op (BigQuery returns the existing result rather than running a new job).  
3. **Idempotent write dispositions** ; temp table loads use WRITE\_TRUNCATE, so reloading a partition replaces rather than duplicates data. The final load/copy uses WRITE\_APPEND because each checkpoint appends a new batch; truncating would destroy data from prior checkpoints.  
4. **Atomic COPY for large data** ; when files exceed single-load-job limits, all partitions load to temp tables and a single COPY job makes all data visible atomically. Either all temp tables are merged into the final table, or none are.

### **Job ID Formats**

Job and temp table IDs embed a UUID that is generated once at operator construction and persists across internal restarts via Java serialization. The `{flinkJobIdHex}` segment is the Flink job ID rendered as a 32-char hex string (`getRuntimeContext().getJobInfo().getJobId().toHexString()`) - already alphanumeric, so no sanitization is needed to satisfy BigQuery's `[a-zA-Z0-9_-]` job ID constraint.

| Type | Flink format |
| ----- | ----- |
| Direct load job | flink\_bq\_load\_{flinkJobIdHex}\_{uuid}\_c{checkpointId} |
| Temp load job | flink\_bq\_tmpload\_{flinkJobIdHex}\_{uuid}\_c{checkpointId}\_p{P} |
| Temp table ID | flink\_bq\_tmp\_{flinkJobIdHex}\_{uuid}\_c{checkpointId}\_p{P} |
| Copy job | flink\_bq\_copy\_{flinkJobIdHex}\_{uuid}\_c{checkpointId} |

**UUID persistence via Java serialization:** RichFlatMapFunction implements Serializable. The UUID is a non-transient field initialized at construction. When Flink serializes the operator into the StreamConfig (via InstantiationUtil), the UUID is included. On **internal restart** (task failure), the operator is deserialized \=\> same UUID \=\> idempotent retries. On **new deployment** (even from savepoint), the operator is constructed fresh \=\> new UUID \=\> no collision with prior execution’s job IDs.

On job failure, the retry index advances (\_r1, \_r2, … up to \_r3) because a failed BigQuery job ID is permanently burned. The same failure scenario always tries the same sequence of IDs, maintaining determinism.

### **Checkpoint Completion Tracking**

The operator groups files by checkpoint ID and tracks completion via CommittableSummary counting. Each checkpoint’s CheckpointState tracks numberOfSubtasks, summariesReceived, totalExpectedCommittables, and committablesReceived. A checkpoint is complete when summariesReceived \== numberOfSubtasks AND committablesReceived \== totalExpectedCommittables. Only complete checkpoints are submitted as load jobs.

### **Failure Scenarios**

| Scenario | What Happens | Recovery |
| ----- | ----- | ----- |
| Writer crashes before checkpoint | In-progress files are abandoned | GCS lifecycle policy cleans up orphans |
| Writer crashes during file finalization | Committable is incomplete, not checkpointed | Flink rolls back to last successful checkpoint |
| Post-commit topology fails to submit load job | Exception propagated, checkpoint not completed | Flink replays from last checkpoint; same committable resubmitted |
| Load job fails on BigQuery side | BigQueryConnectorException thrown | Flink replays checkpoint; retry index advances to \_r1, \_r2, etc. |
| Load job succeeds but GCS cleanup fails | Processing continues (cleanup is best-effort) | Files remain; GCS lifecycle policy deletes them |
| Flink replays a completed checkpoint | Same checkpoint ID \=\> same files \=\> same job ID | BigQuery returns existing successful result; no data duplication |
| Copy job fails (large data path) | Exception propagated | Flink replays; temp tables reloaded with WRITE\_TRUNCATE (idempotent) |
| Checkpoint messages arrive interleaved (streaming) | Incomplete checkpoint is not submitted | Eager submission triggers once the final message makes the checkpoint complete |
| Streaming job restarted from savepoint | New operator instance \=\> new UUID | No collision with prior execution’s job IDs; new data loaded normally |
| Second deployment of same job | New operator instance \=\> new UUID | No collision with prior execution’s job IDs |
| Same table written from multiple sinks | Different operator instances \=\> different UUIDs | Each sink’s job IDs are independent |

## **Key Design Decisions**

### **1\. Support both batch and streaming mode**

**Decision:** Support both batch and streaming mode. 

**Rationale:** Streaming support falls out of the FileSink composition essentially for free, so the sink supports both modes, but indirect writes are a poor fit for typical streaming use cases. Streaming workloads usually require low-latency results, and BigQuery load jobs operate at minute-level granularity. Coupling load jobs with checkpointing also makes choosing checkpoint frequency and checkpoint-timeout durations non-trivial. BigQuery's per-table and per-project load-job quotas are also easy to exhaust at high checkpoint frequencies.

### **2\. FileSink \+ post-commit topology**

**Decision:** Compose FileSink\<RowData\> for staging files on GCS and use SupportsPostCommitTopology\<FileSinkCommittable\> to submit BigQuery load jobs after files are committed.

**Risks:**

* Although SupportsPostCommitTopology is annotated @Experimental, it is being used in other major OSS connectors e.g. Apache Iceberg’s new IcebergSink. There is an open JIRA to promote it to @PublicEvolving: [FLINK-37526](https://issues.apache.org/jira/browse/FLINK-37526).
* There is an issue with SupportsPostCommitTopology that makes streaming \+ bounded use cases challenging (though not impossible) to support: [https://issues.apache.org/jira/browse/FLINK-39192](https://issues.apache.org/jira/browse/FLINK-39192)   
* SupportsPostCommitTopology is only available in the Flink 2.1 version of the connector, so these changes cannot be backported to earlier Flink versions.

**Alternatives:**

* **Fully custom sink** A custom Sink implementation would require writing a custom SinkWriter, Committer, committable type, and committable serializer \- essentially reimplementing what FileSink already provides for file staging. The main motivation would be to trigger load jobs from inside Committer.commit(), but the Committer API doesn’t expose checkpointId or subtaskId, which are needed for deterministic job IDs. You’d have to smuggle them through a custom committable wrapper. The post-commit topology avoids all of this: CommittableWithLineage already carries checkpoint and subtask info via Flink’s infrastructure, and FileSink handles all the file lifecycle (in-progress \=\> pending \=\> committed) with battle-tested code.  
* **Why not use inheritance (instead of composition)?** FileSink has a **private constructor** ( built via FileSink.forBulkFormat(...).build()), so inheritance is impossible.

### **3\. Exactly-once delivery guarantee**

**Decision:** The sink provides exactly-once delivery, not at-least-once.

**Rationale:**

At-least-once semantics would mean duplicate rows can appear in the destination table on failure and retry. While duplicates are often acceptable in streaming pipelines, they are unacceptable for batch use cases. Consider a job that reads a table with 1,000 rows, updates a column value for each row, and writes the results back to BigQuery. With at-least-once, a retry could produce 1,050 rows in the output table ; the original 1,000 plus 50 duplicates from a replayed partition. The row count no longer matches the input, silently corrupting the dataset. Since the primary motivation for this feature is batch workloads, the sink targets exactly-once.

### **4\. Two-path loading: small data vs. large data**

**Decision:** When all files fit within a single load job’s limits (10,000 files, 15 TB), load directly to the final table. When limits are exceeded, partition files across multiple temp tables (each with WRITE\_TRUNCATE) and atomically COPY to the final table.

**Rationale:**

* The small data path is expected to be the common case \- most checkpoints produce well under 10,000 files.  
* The multi-partition path is needed when limits are exceeded because loading multiple jobs directly to the final table breaks atomicity (data appears incrementally). Temp tables \+ a single COPY job make all data visible at once.  
* This mirrors Beam's approach. Spark does not support the large-data path.

**Alternatives:**

* **Always use temp tables \+ copy.** Reduces code to a single path, but doubles quota usage (temp load \+ copy instead of one direct load per checkpoint), which is wasteful in the common case.  
* **Single shared temp table with WRITE\_APPEND.** Fewer temp tables to manage, but breaks retry idempotency: if the copy job fails and the commit retries, previously completed partition loads are skipped (idempotent via job ID) while their data remains in the temp table. Retried partitions (with new job IDs) append again, causing duplicates.

### **5\. Couple load jobs with checkpointing**

**Decision:** Load jobs are submitted synchronously - `waitForJob()` blocks the mailbox thread until the load job completes, which means the checkpoint barrier cannot advance past the operator until the load job’s outcome is known.

**Why coupling works:** When `waitForJob()` blocks, the checkpoint barrier is held at the operator. If the load job succeeds, the barrier advances and the checkpoint completes - the load is now durably committed. If the load job fails, the exception propagates, the checkpoint fails, and Flink rolls back to the last successful checkpoint. On replay, the same checkpoint ID produces the same files and the same deterministic job ID, so BigQuery’s immutable job ID semantics make the retry idempotent.

**Why decoupling would break recovery:** If load jobs were submitted asynchronously (decoupled from checkpoints), the following data-loss scenario becomes possible:

*Scenario A - fire-and-forget:*

1. Checkpoint N completes - files are staged on GCS, load job is submitted but not yet finished
2. Checkpoint N+1’s barrier arrives and advances through the operator (since we’re not blocking on N’s load job)
3. Checkpoint N+1 completes successfully
4. Checkpoint N’s load job fails (or the job crashes before N’s load completes)
5. On recovery, Flink restores from checkpoint N+1 (the last successful checkpoint)
6. Checkpoint N’s files are now orphaned - they are before the restore point, so Flink will never replay them
7. **Result: data loss.** Checkpoint N’s rows never make it to BigQuery.

*Scenario B - ordered submission (don’t submit N+1’s load job until N’s succeeds), stateless operator:*

1. Checkpoint N completes - files are staged on GCS, load job N submitted
2. Checkpoint N+1’s barrier arrives and advances through the operator - checkpoint N+1 completes, but load job N+1 is queued (waiting for N’s load job to succeed first)
3. Load job N fails - operator throws exception - task fails
4. On recovery, Flink restores from checkpoint N+1 (the last successful checkpoint)
5. On replay from checkpoint N+1, Flink re-delivers N+1’s committables - but **not** N’s committables (they belong to a checkpoint before the restore point)
6. The operator has no memory of N’s failed load job (operator state is transient, not checkpointed)
7. **Result: data loss.** Same as Scenario A - checkpoint N’s rows never make it to BigQuery.

*Scenario C - ordered submission with stateful operator (operator durably checkpoints pending load job status and file lists):*

1. Checkpoint N completes - files are staged on GCS, load job N submitted
2. Checkpoint N+1’s barrier arrives and advances through the operator - checkpoint N+1 completes, and the operator’s state is checkpointed including: {N → [files, PENDING], N+1 → [files, QUEUED]}
3. Load job N fails - operator throws exception - task fails
4. On recovery, Flink restores from checkpoint N+1 - operator state is restored with {N → [files, PENDING], N+1 → [files, QUEUED]}
5. Operator retries N’s load job using the file list from its restored state (files still exist on GCS - cleanup is best-effort and only happens after success)
6. N succeeds - operator submits N+1’s load job
7. **Result: no data loss**, but this requires the operator to implement CheckpointedFunction with a durable pending-job queue, its own retry logic with deterministic job IDs, and ordered submission tracking - essentially reimplementing a durable job queue inside a Flink operator. This is significantly more complex than the current synchronous approach but may be worth exploring if streaming support becomes a higher priority.

**Trade-off:** Coupling means load job duration directly impacts checkpoint latency. If a load job takes longer than the checkpoint timeout, the checkpoint will fail. This makes streaming mode awkward - users must set the checkpoint timeout high enough to accommodate the longest expected load job (which can take seconds to minutes depending on data size). This is a key reason indirect writes are recommended primarily for batch workloads.

### **6\. One load job per complete checkpoint**

**Decision:** One load job per complete checkpoint, submitted as soon as the checkpoint is complete. The operator collects files by checkpoint ID and tracks completion via CommittableSummary counting.

**Alternatives:**

* **Per file** ; each CommittableWithLineage carries one file. Submitting a load job per file would quickly exhaust BigQuery’s daily job quota.

### **7\. Best-effort GCS cleanup**

**Decision:** Delete GCS files immediately after successful load (best-effort). Failures are logged but don’t fail the pipeline. Rely on GCS bucket lifecycle policies as the safety net.

**Rationale:** GCS cleanup is not part of the correctness contract; files are only needed until the load job succeeds. Making cleanup failures fatal would create unnecessary pipeline restarts. A GCS lifecycle policy (e.g. delete objects \> 7 days) handles orphaned files from crashes. This matches Beam's approach. Lifecycle rules are needed regardless to clean up orphans from unclean shutdowns.

**Alternatives:**

* **Guaranteed cleanup (fail pipeline on delete failure)** ; cleanup is not part of the correctness contract, so failing the pipeline would cause unnecessary restarts. Worse, on restart the load job would be retried idempotently but the files might already be partially deleted, and _may_ require complex bookkeeping to track which files still need cleanup.

### **8\. No support for overwrite semantics initially**

**Decision:** The sink always uses WRITE\_APPEND for the final destination table. We may consider supporting WRITE\_TRUNCATE (overwrite) semantics in the future as a follow-up.

**Rationale:**

In batch mode, WRITE\_TRUNCATE is technically feasible: Flink always produces exactly one end-of-input checkpoint, so there’s only one load job that could use WRITE\_TRUNCATE to replace the table.

Streaming mode needs more consideration. WRITE\_TRUNCATE on each checkpoint would be unusual: each checkpoint would destroy all data from prior checkpoints, leaving only the latest batch in the table. For comparison, in indirect-mode Beam only applies WRITE\_TRUNCATE to the first trigger pane and auto-downgrades to WRITE\_APPEND for all subsequent panes.

Note the existing BigQuerySink (direct writes via Storage Write API) only supports append-only semantics.

### **9\. Parquet as the staging file format**

**Decision:** Write Snappy-compressed Parquet files to GCS via Flink's `flink-parquet` module 

**Rationale:**

1. Excellent type fidelity for the BigQuery types  
2. Flink's battle-tested `flink-parquet` module ships a `RowData`-aware Parquet builder, so we don't have to maintain our own row-to-record converter  
3. Snappy compression keeps the staging-bucket footprint small

**BigQuery-side configuration** (see `BigQueryLoadJobOperator.java`):

* `ParquetOptions.setEnableListInference(true)` - Parquet's standard `LIST` encoding nests array elements under a `{list/element}` group. Without this BigQuery surfaces the column as a `RECORD` instead of a repeated/array column and the load fails with "changed type from INTEGER to RECORD".
* `setDecimalTargetTypes(["BIGNUMERIC", "NUMERIC", "STRING"])` - the default order picks `NUMERIC` for any decimal with precision ≤ 38, even when scale > 9, which mismatches a target `BIGNUMERIC` column. Putting `BIGNUMERIC` first preserves the column type when source decimals have scale > 9.

**Caveat:** BigQuery's Parquet load reads both Flink `TIMESTAMP` and `TIMESTAMP_LTZ` as `TIMESTAMP` - DATETIME target columns aren't supported currently

**Alternatives considered:**

BigQuery load jobs accept Avro, Parquet, JSON, ORC, and CSV files.

| Criteria | Avro | Parquet | JSON | ORC | CSV |
| ----- | ----- | ----- | ----- | ----- | ----- |
| **Compression codecs** | Snappy, DEFLATE, ZSTD | Snappy, GZip, LZO, LZ4\_RAW, ZSTD | Gzip only (max 4 GB) | Snappy, Zlib, LZO, LZ4, ZSTD | Gzip only (max 4 GB) |
| **Schema** | Self-describing; auto-detected | Self-describing; auto-detected | Newline-delimited; auto-detection available | Self-describing; auto-detected | Not self-describing; auto-detection limited (fails if all columns string) |
| **Nested/repeated data** | Supported | Supported | Supported | Supported | Not supported |
| **BigQuery type fidelity** | Full via useAvroLogicalTypes | Good ; most types map, some require annotation | String-based ; requires schema specification; large integers can lose precision | Good ; most types map, similar to Avro | String-based ; all values parsed from text; date/time formats required |
| **Flink ecosystem support** | flink-avro module | flink-parquet module | Only raw string writing | flink-orc module | flink-csv module |
| **What Beam uses** | Supported (default) | Unsupported | Supported | Unsupported | Unsupported |
| **What Spark uses** | Supported | Supported (default) | Unsupported | Supported | Unsupported |

### **10\. Deterministic file path extraction via CommittableWithLineage**

**Decision:** Extract finalized GCS file paths from `CommittableWithLineage<FileSinkCommittable>` in the post-commit topology.  

**Rationale:** With SupportsPostCommitTopology, the post-commit topology receives CommittableMessage\<FileSinkCommittable\> which includes CommittableWithLineage \- this already carries the FileSinkCommittable with the finalized file path, checkpointed and replayed exactly on recovery. checkpointId from CommittableWithLineage/CommittableSummary is used to group files by checkpoint and include the checkpoint ID in deterministic job IDs (see Decisions 5 and 6).

**Alternatives considered:**

* **GCS directory listing.** List the GCS directory to discover finalized files. This is fragile: multiple subtasks could write to the same base directory, so a listing could pick up files from other subtasks or from previous (failed) checkpoints, causing race conditions and cross-subtask interference.

### **11\. What rolling policy should we use for the underlying FileSink?**

**Decision:** Custom SizeBasedCheckpointRollingPolicy that rolls on every checkpoint or when a file exceeds 1.5 GB.

**Rationale:** The 1.5 GB threshold ensures the single-partition load path remains viable for the common case. BigQuery imposes a constraint of 10,000 source URIs and 15 TB per load job. At 1.5 GB per file, a 15 TB dataset hits exactly 10,000 files ; the maximum the single-partition path supports. A smaller threshold (e.g. 1 GB) would force the multi-partition temp-table path more often than necessary.

### **12\. Why RichFlatMapFunction for the post-commit operator?**

**Decision:** BigQueryLoadJobOperator extends RichFlatMapFunction and is wired via `committables.global().flatMap(...).forceNonParallel()`. `.global()` routes every subtask's committables to a single instance of the operator and `.forceNonParallel()` pins it to parallelism 1.

**Rationale:** The operator just accumulates files in a HashMap and submits load jobs ; no timers, no Flink managed state, no output. RichFlatMapFunction is the simplest API fit for this use case. Forcing it to a single non-parallel instance means there's exactly one load job per complete checkpoint (no per-subtask fan-out), which keeps the daily-job-quota math tractable and avoids any cross-subtask coordination of file batching.

**Alternatives considered:**

* **Sink v2 API (.sinkTo())**; SinkWriterOperator throws an exception when used in PostCommitTopology with a bounded source and streaming mode: https://issues.apache.org/jira/browse/FLINK-39192  
* **Custom AbstractStreamOperator**; works but unnecessarily heavy for an operator with no timers, state, or output.  
* **KeyedProcessFunction**; requires faking a keyed stream; unnecessary complexity.

## **Appendix**

### **Relevant BigQuery Load Job Limits**

The following BigQuery limits constrain how the sink operates. The partitioning mechanism handles the file-count and byte-size limits automatically, but operators must be aware of daily quotas.

| Limit | Value | Impact on Sink |
| ----- | ----- | ----- |
| **Load jobs per table per day** | 1,500 | The post-commit operator is wired `.global().forceNonParallel()`, so a single instance receives committables from every subtask and submits **one** load job per complete checkpoint (regardless of write parallelism). At 1-minute checkpoints that’s \~1,440/day - close to the limit. Mitigate by increasing the checkpoint interval. |
| **Load jobs per project per day** | 100,000 | Shared across all tables. Multiple sinks or high-frequency checkpoints can exhaust this. |
| **Max files per load job** | 10,000 | Handled automatically by partitioning into chunks of 10,000. |
| **Max size per load job** | 15 TB | Handled automatically by partitioning based on tracked file sizes. |
| **Max source URIs in job config** | 10,000 | Equals the file limit; handled by the same partitioning. |
| **Load job execution time limit** | 6 hours | Very large loads may hit this. No mitigation in the sink. |
| **Max columns per table** | 10,000 | Schema-level limit; not specific to the sink. |

**Key operational concern:** The **1,500 load jobs per table per day** limit is the most likely to be hit. Because the operator is non-parallel, every complete checkpoint triggers exactly one load job (the large-data path will fan out into a few extra temp-table loads + one COPY, but the dominant cost is still per-checkpoint). Since load jobs are submitted eagerly on checkpoint completion, the checkpoint interval directly controls the load job submission rate. Users should tune:
* **Checkpoint interval:** Longer intervals \= fewer load jobs but higher latency.  
* **Target rate:** With a 1-minute checkpoint interval, that’s \~1,440 jobs/day ; near the limit. A 5-minute interval yields \~288 jobs/day (safely within limit).

### **Do post-commit topology operators participate in checkpointing?**

**Yes; they behave identically to any other operator in the pipeline.** Post-commit topology operators are full participants in the checkpoint cycle: they receive checkpoint barriers, execute snapshotState(), and must complete their checkpoint work within the configured timeout. There is no special exemption or isolation for post-commit operators. The following behaviour was confirmed via experiments:

1. A ProcessFunction implementing CheckpointedFunction in the post-commit topology has its snapshotState() called during checkpoints. This proves checkpoint barriers flow through the post-commit topology.  
2. Blocking forever in element processing causes checkpoint failures (verified via the Flink REST API’s /jobs/{jobId}/checkpoints endpoint showing counts.failed \> 0). The mailbox thread is stuck, so subsequent checkpoint barriers cannot be processed.  
3. Blocking forever in snapshotState() causes the same checkpoint timeout failure. The checkpoint cannot complete while snapshotState() is blocked.

**Conclusion:** Post-commit topology operators are not “fire-and-forget” ; they are wired into the same checkpoint mechanism as every other operator. snapshotState() is called, checkpoint barriers must be processed, and blocking the mailbox thread (whether in flatMap() or snapshotState()) blocks checkpoints from completing successfully. 

**Implication for this sink:** BigQueryLoadJobOperator.flatMap() calls waitForJob() which blocks the mailbox thread while polling a BigQuery load job. This means **load job duration directly affects checkpoint latency**. If a load job takes longer than the checkpoint timeout, the checkpoint will fail. Operators must ensure:

* **Checkpoint timeout** is set high enough to accommodate the longest expected load job (load jobs can take seconds to minutes depending on data size).  
* **Checkpoint interval** is long enough that load jobs from the previous checkpoint complete before the next checkpoint’s barriers arrive at the post-commit operator.

As noted above, this sink is best suited to batch applications.
