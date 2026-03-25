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

As always, there is no such thing as a free lunch:

* **Higher latency.** Load jobs can take multiple minutes whereas writing via the Storage Write API is in the seconds range.  
* **No guaranteed capacity.** Google does not guarantee there will always be available capacity in the shared slot pool. 

For these reasons, we expect indirect writes to be a better fit for **batch** workloads where latency in the order of minutes is acceptable.

## **Architecture**

### **High Level Flow**

```
flowchart LR
    subgraph Flink Job
        A[Input Records<br/>RowData] --> B[FileSink's FileWriter<br/>per subtask]
        B -->|Avro files| C[(GCS Staging<br/>Bucket)]
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
    participant GCS as GCS (Avro files)
    participant FC as FileCommitter (via FileSink)
    participant LJ as BigQueryLoadJobOperator
    participant BQ as BigQuery
    R ->> W: write(RowData)
    W ->> W: RowDataAvroWriterFactory converts to GenericRecord
    W ->> GCS: append to Avro file
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
    participant GCS as GCS (Avro files)
    participant FC as FileCommitter (via FileSink)
    participant LJ as BigQueryLoadJobOperator
    participant BQ as BigQuery

    loop Per checkpoint
        R ->> W: write(RowData)
        W ->> GCS: append to Avro file
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
C --> C1["LoadJob(finalTable, allUris)<br/>job ID: flink_bq_load_{hash}_cN_{uriHash}"]
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
2. **Deterministic job IDs** ; job IDs embed a UUID that persists across internal restarts, plus the checkpoint ID and subtask index. On replay, the same operator instance produces the same job ID. BigQuery job IDs are globally unique and immutable: once a job ID completes, resubmitting the same ID is a no-op (BigQuery returns the existing result rather than running a new job).  
3. **Idempotent write dispositions** ; temp table loads use WRITE\_TRUNCATE, so reloading a partition replaces rather than duplicates data. The final load/copy uses WRITE\_APPEND because each checkpoint appends a new batch; truncating would destroy data from prior checkpoints.  
4. **Atomic COPY for large data** ; when files exceed single-load-job limits, all partitions load to temp tables and a single COPY job makes all data visible atomically. Either all temp tables are merged into the final table, or none are.

### **Job ID Formats**

Job and temp table IDs embed a UUID that is generated once at operator construction and persists across internal restarts via Java serialization. The Flink job name is sanitized (non-alphanumeric chars replaced with \_, truncated to 100 chars) to comply with BigQuery’s \[a-zA-Z0-9\_-\] job ID constraint.

| Type | Flink format |
| ----- | ----- |
| Direct load job | flink\_bq\_load\_{jobName}\_{uuid}\_c{checkpointId}\_s{subtaskId} |
| Temp load job | flink\_bq\_tmpload\_{jobName}\_{uuid}\_c{checkpointId}\_s{subtaskId}\_p{P} |
| Temp table ID | flink\_bq\_tmp\_{jobName}\_{uuid}\_c{checkpointId}\_s{subtaskId}\_p{P} |
| Copy job | flink\_bq\_copy\_{jobName}\_{uuid}\_c{checkpointId}\_s{subtaskId} |

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

**Rationale:** The question here is whether or not indirect writes should be supported in streaming mode. I’ve added support for it for now *incidentally* but we’ll have to see how well it really works in practice. Typically, when you’re streaming, you want results with low latency and load jobs are decidedly not low latency. In addition, I foresee users having challenges with figuring out the right frequency of checkpointing and checkpoint-timeout durations if load jobs are coupled with checkpointing. Last but not least, given the various quotas/limits associated with load job operations, it's easy for users to end up foot-gunning themselves.   

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

At-least-once semantics would mean duplicate rows can appear in the destination table on failure and retry. While in streaming land, duplicates are often considered ok, this is unacceptable for batch use cases. Consider a job that reads a table with 1,000 rows, updates a column value for each row, and writes the results back to BigQuery. With at-least-once, a retry could produce 1,050 rows in the output table ; the original 1,000 plus 50 duplicates from a replayed partition. The row count no longer matches the input, silently corrupting the dataset. Given the primary motivation for this feature is for batch workloads, we should aim for exactly-once. 

### **4\. Two-path loading: small data vs. large data**

**Decision:** When all files fit within a single load job’s limits (10,000 files, 15 TB), load directly to the final table. When limits are exceeded, partition files across multiple temp tables (each with WRITE\_TRUNCATE) and atomically COPY to the final table.

**Rationale:**

* The small data path is expected to be the common case \- most checkpoints produce well under 10,000 files.  
* The multi-partition path is needed when limits are exceeded because loading multiple jobs directly to the final table breaks atomicity (data appears incrementally). Temp tables \+ a single COPY job make all data visible at once.  
* This is similar to how Beam does it. Spark does not support large data.

**Alternatives:**

* **Always use temp tables \+ copy.** Reduces code to a single path, but doubles quota usage (temp load \+ copy instead of one direct load per checkpoint). Seems wasteful.  
* **Single shared temp table with WRITE\_APPEND.** Fewer temp tables to manage, but breaks retry idempotency: if the copy job fails and the commit retries, previously completed partition loads are skipped (idempotent via job ID) while their data remains in the temp table. Retried partitions (with new job IDs) append again, causing duplicates.

### **5\. Submit load jobs on checkpoint**

**Decision:** One load job per complete checkpoint, submitted as soon as the checkpoint is complete. The operator collects files by checkpoint ID and tracks completion via CommittableSummary counting.

**Alternatives:**

* **Per file** ; each CommittableWithLineage carries one file. Submitting a load job per file would quickly exhaust BigQuery’s daily job quota.  
* **On a timer decoupled from checkpoint** ; adds a config knob and delays submission up to the timer interval. The checkpoint interval already controls commit frequency, so an additional timer is unnecessary complexity. In addition, decoupling from checkpointing would make recovery from failure scenarios more challenging.

### **6\. Best-effort GCS cleanup**

**Decision:** Delete GCS files immediately after successful load (best-effort). Failures are logged but don’t fail the pipeline. Rely on GCS bucket lifecycle policies as the safety net.

**Rationale:** GCS cleanup is not part of the correctness contract; files are only needed until the load job succeeds. Making cleanup failures fatal would create unnecessary pipeline restarts. A GCS lifecycle policy (e.g. delete objects \> 7 days) handles orphaned files from crashes. This is the same approach **Beam uses**. There’s always going to be orphaned files potentially anyway so you will need this rule anyway.

**Alternatives:**

* **Guaranteed cleanup (fail pipeline on delete failure)** ; cleanup is not part of the correctness contract, so failing the pipeline would cause unnecessary restarts. Worse, on restart the load job would be retried idempotently but the files might already be partially deleted, and _may_ require complex bookkeeping to track which files still need cleanup.

### **7\. No support for overwrite semantics initially**

**Decision:** The sink always uses WRITE\_APPEND for the final destination table. We may consider supporting WRITE\_TRUNCATE (overwrite) semantics in the future as a follow-up.

**Rationale:**

In batch mode, WRITE\_TRUNCATE is technically feasible: Flink always produces exactly one end-of-input checkpoint, so there’s only one load job that could use WRITE\_TRUNCATE to replace the table.

Streaming mode needs a bit more thinking. WRITE\_TRUNCATE on each checkpoint would be unusual as that would mean each checkpoint would destroy all data from prior checkpoints, leaving only the latest batch in the table. For comparison, in indirect-mode Beam only applies WRITE\_TRUNCATE to the first trigger pane and auto-downgrades to WRITE\_APPEND for all subsequent panes.

Note the existing BigQuerySink (direct writes via Storage Write API) only supports append-only semantics.

### **8\. Avro as the staging file format**

**Decision:** Write Avro binary files (Snappy-compressed) to GCS. We can offer Parquet support in the future if needed.

**Rationale:**

1. Beam’s BigQuery indirect-write method also defaults to Avro file format  
2. Excellent type fidelity  
3. We can re-use Flink’s battle-tested flink-avro module  
4. The connector already uses avro library dependencies for reading data

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

### **9\. Deterministic file path extraction via CommittableWithLineage**

**Decision:** Extract finalized GCS file paths from CommittableWithLineage\<FileSinkCommittable\> in the post-commit topology via FileSinkCommittable.getPendingFile().getPath().

**Rationale:** With SupportsPostCommitTopology, the post-commit topology receives CommittableMessage\<FileSinkCommittable\> which includes CommittableWithLineage \- this already carries the FileSinkCommittable with the finalized file path, checkpointed and replayed exactly on recovery. checkpointId from CommittableWithLineage/CommittableSummary is used to group files by checkpoint and include the checkpoint ID in deterministic job IDs (see Decision 5).

**Alternatives considered:**

* **GCS directory listing.** List the GCS directory to discover finalized files. This is fragile: multiple subtasks could write to the same base directory, so a listing could pick up files from other subtasks or from previous (failed) checkpoints, causing race conditions and cross-subtask interference.

### **10\. What rolling policy should we use for the underlying FileSink?**

**Decision:** Custom SizeBasedCheckpointRollingPolicy that rolls on every checkpoint or when a file exceeds 2 GB.

**Rationale:** 2 GB file threshold ensures that as much as possible the single-partition load path is viable. BigQuery has a constraint of 10,000 source URIs per load job. At 2 GB per file, a 15 TB dataset produces \~7,500 files ; under the limit. At 1 GB it would produce \~15,000, forcing the multi-partition temp-table path unnecessarily.

### **11\. Why RichFlatMapFunction for the post-commit operator?**

**Decision:** BigQueryLoadJobOperator extends RichFlatMapFunction and is wired via .flatMap().

**Rationale:** The operator just accumulates files in a HashMap and submits load jobs ; no timers, no Flink managed state, no output. RichFlatMapFunction is the simplest API fit for this use case.

**Alternatives considered:**

* **Sink v2 API (.sinkTo())**; SinkWriterOperator throws an exception when used in PostCommitTopology with a bounded source and streaming mode: https://issues.apache.org/jira/browse/FLINK-39192  
* **Custom AbstractStreamOperator**; works but unnecessarily heavy for an operator with no timers, state, or output.  
* **KeyedProcessFunction**; requires faking a keyed stream; unnecessary complexity.

## **Implementation Plan**

This feature could be broken up into the following PRs: 
1. Add support for indirect writes in batch mode for data up to 15 TB
1. Add support for indirect writes in batch mode for larger than 15 TB data
1. Add support for streaming mode with unbounded datasets
    - Basically test in streaming mode and document gotchas
1. Add support for streaming mode with bounded datasets
    - Basically test in streaming mode and document gotchas
    - We may need some additional logic in `BigQueryLoadJobOperator.close()` due to an issue similar to https://issues.apache.org/jira/browse/FLINK-39192
1. Add support for ENABLE_TABLE_CREATION
1. Add support for WRITE_TRUNCATE
1. Add retry logic for BigQuery jobs if needed:
   - Basically append _r1, _r2, _r3 at the end of BigQuery jobs because BigQuery jobs are idempotent

## **Appendix**

### **Relevant BigQuery Load Job Limits**

The following BigQuery limits constrain how the sink operates. The partitioning mechanism handles the file-count and byte-size limits automatically, but operators must be aware of daily quotas.

| Limit | Value | Impact on Sink |
| ----- | ----- | ----- |
| **Load jobs per table per day** | 1,500 | Each checkpoint that produces files triggers at least 1 load job per subtask. With 4 subtasks and 1-minute checkpoints, that’s \~5,760 jobs/day ; **exceeds the limit**. Mitigate by increasing checkpoint interval or reducing parallelism. |
| **Load jobs per project per day** | 100,000 | Shared across all tables. Multiple sinks or high-frequency checkpoints can exhaust this. |
| **Max files per load job** | 10,000 | Handled automatically by partitioning into chunks of 10,000. |
| **Max size per load job** | 15 TB | Handled automatically by partitioning based on tracked file sizes. |
| **Max source URIs in job config** | 10,000 | Equals the file limit; handled by the same partitioning. |
| **Load job execution time limit** | 6 hours | Very large loads may hit this. No mitigation in the sink. |
| **Avro data block max size** | 16 MB | Flink’s AvroWriters respects this by default. |
| **Max columns per table** | 10,000 | Schema-level limit; not specific to the sink. |

**Key operational concern:** The **1,500 load jobs per table per day** limit is the most likely to be hit. Each checkpoint that produces files triggers at least one load job (or more if the large-data path is triggered). Since load jobs are submitted eagerly on checkpoint completion, the checkpoint interval directly controls the load job submission rate. Users should tune:
* **Checkpoint interval:** Longer intervals \= fewer load jobs but higher latency.  
* **Parallelism:** Fewer subtasks \= fewer load jobs per checkpoint (files from all subtasks are batched into a single load job).  
* **Target rate:** With a 1-minute checkpoint interval, that’s \~1,440 jobs/day ; near the limit. A 5-minute interval yields \~288 jobs/day (safely within limit).

### **Do post-commit topology operators participate in checkpointing?**

**Yes; they behave identically to any other operator in the pipeline.** Post-commit topology operators are full participants in the checkpoint cycle: they receive checkpoint barriers, execute snapshotState(), and must complete their checkpoint work within the configured timeout. There is no special exemption or isolation for post-commit operators.Confirmed the following behaviour via experiments:

1. A ProcessFunction implementing CheckpointedFunction in the post-commit topology has its snapshotState() called during checkpoints. This proves checkpoint barriers flow through the post-commit topology.  
2. Blocking forever in element processing causes checkpoint failures (verified via the Flink REST API’s /jobs/{jobId}/checkpoints endpoint showing counts.failed \> 0). The mailbox thread is stuck, so subsequent checkpoint barriers cannot be processed.  
3. Blocking forever in snapshotState() causes the same checkpoint timeout failure. The checkpoint cannot complete while snapshotState() is blocked.

**Conclusion:** Post-commit topology operators are not “fire-and-forget” ; they are wired into the same checkpoint mechanism as every other operator. snapshotState() is called, checkpoint barriers must be processed, and blocking the mailbox thread (whether in flatMap() or snapshotState()) blocks checkpoints from completing successfully. 

**Implication for this sink:** BigQueryLoadJobOperator.flatMap() calls waitForJob() which blocks the mailbox thread while polling a BigQuery load job. This means **load job duration directly affects checkpoint latency**. If a load job takes longer than the checkpoint timeout, the checkpoint will fail. Operators must ensure:

* **Checkpoint timeout** is set high enough to accommodate the longest expected load job (load jobs can take seconds to minutes depending on data size).  
* **Checkpoint interval** is long enough that load jobs from the previous checkpoint complete before the next checkpoint’s barriers arrive at the post-commit operator.

Again, this sink is expected to be best used for batch applications.
