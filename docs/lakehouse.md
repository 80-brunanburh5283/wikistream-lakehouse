# Iceberg on a laptop: commits, files, and what maintenance moved

This page is about storage mechanics — how many files a streaming write leaves
behind, what compaction did to them, and why three reasonable ways of measuring the
same table's size give three different answers. The column-level contracts are in
[data-contracts.md](data-contracts.md); ingest rate and compression are in
[throughput.md](throughput.md).

Every number here was produced by a command shown next to it, on 2026-09-17, against
`bronze.recentchange_raw` (450,725 rows) and `silver.edits` (445,395 rows).

## The unit of work is a commit, not a write

Iceberg has no partial state. A micro-batch either lands as a snapshot or does not
exist, and that atomicity is what makes the restart proof in
[correctness.md](correctness.md) possible at all. It is also the source of every
storage problem on this page, because one micro-batch is one commit, and each commit
writes

- one Parquet data file per partition per writing task,
- one manifest listing those files,
- one manifest list naming the manifests in the new snapshot,
- one new `metadata.json` for the table.

At a 30-second trigger that is 2,880 commits a day per table. Freshness is paid for
in metadata, and the two settings that hold the bill down are both compromises:

| Setting | Value | What it trades |
|---|---|---|
| `WS_TRIGGER_INTERVAL_SECONDS` | 30 | Latency for file count. A 5-second trigger would cut end-to-end lag by 25 seconds and produce six times the files. |
| `spark.sql.shuffle.partitions` | 4 | Parallelism for file count. Spark's default is 200 whatever the machine is, and 200 writing tasks on a batch of a few thousand rows produce 200 files holding what one file should. |
| `write.metadata.delete-after-commit.enabled` | `true` | Unbounded metadata history for a 20-version window. Without it, 2,880 `metadata.json` files a day accumulate and never leave. |

## Small files are not a mistake here, they are arithmetic

40 events/second at roughly 300 compressed bytes each is 12 KB/s. A 30-second
micro-batch therefore has about 360 KB to write, against a
`write.target-file-size-bytes` of 128 MiB. No amount of tuning makes a 360 KB batch
produce a 128 MiB file; the target only becomes reachable when something rewrites
many batches together. That is what compaction is for, and it is why the streaming
job does not attempt to size its own output.

Measured, immediately before the maintenance run below:

| Table | Data files | Average file | Total |
|---|---|---|---|
| `bronze.recentchange_raw` | 73 | 981.8 KiB | 70.0 MiB |
| `silver.edits` | 92 | 318.6 KiB | 28.6 MiB |

Silver's files are a third the size of bronze's from the same events, because silver
drops `raw_payload` — the whole original JSON — and keeps the typed columns.

## Compaction: what `make maintain` did

```bash
make maintain
```

Four procedures in order: `rewrite_data_files`, `rewrite_manifests`,
`expire_snapshots`, `remove_orphan_files`. Both tables hold a single day, and both
partition by day, so each collapsed to one partition's worth of one file.

| | bronze before | bronze after | silver before | silver after |
|---|---|---|---|---|
| data files | 73 | **1** | 92 | **1** |
| average file size | 981.8 KiB | **71,060.8 KiB** | 318.6 KiB | **28,667.1 KiB** |
| total size | 70.0 MiB | 69.4 MiB | 28.6 MiB | 28.0 MiB |
| manifests | 20 | **1** | 24 | **1** |
| snapshots | 42 | 44 | 23 | 25 |
| rows | 450,725 | 450,725 | 445,395 | 445,395 |

Three things in that table are worth saying out loud.

**Total size barely moved: 70.0 MiB to 69.4 MiB, under one percent.** Compaction is
not a compression win. zstd was already applied per file, and merging files does not
find much more redundancy. What compaction buys is planning cost and read
amplification: one file open instead of 73, one manifest to scan instead of 20.
`rewrite_data_files` reported `rewritten_bytes_count=73392258` — it moved 70 MiB of
data to save 0.6 MiB of space, which is only sensible if you understand it is not
buying space.

**Snapshot count went up, not down.** A rewrite is itself a commit; so is a manifest
rewrite. Compaction adds two snapshots before expiry removes any, and the after
column is measured between the two procedures.

**The compacted file is 69.4 MiB against a 128 MiB target, and stops there.** There
is nothing left to merge it with. A reader should take the file *count* from this
table and not the absolute sizes — see the limitations below.

## Snapshot expiry, and the floor under it

`expire_snapshots` is what actually reclaims disk, and it is the one procedure that
can destroy something a reader wanted: every expired snapshot is a timestamp you can
no longer time-travel to. Two guards:

- `history.expire.max-snapshot-age-ms` is 7 days, so age alone will not remove
  yesterday's snapshot.
- `RETAIN_LAST_SNAPSHOTS = 5` in `src/wikistream/maintenance.py` is a floor on
  count, independent of age. It exists so the time-travel example in
  [correctness.md](correctness.md) still has a previous snapshot to query
  immediately after a maintenance run — a demo that only works before you run
  maintenance is not a demo.

Run deliberately with the age threshold at zero, to show the mechanism rather than
wait a week:

```bash
make maintain MAINTAIN_ARGS="--snapshot-age-hours 0"
```

| | bronze | silver |
|---|---|---|
| snapshots | 44 → **5** | 25 → **5** |
| data files deleted | 78 | 0 |
| manifests deleted | 35 | 0 |
| manifest lists deleted | 39 | 20 |

The asymmetry is the interesting part. Expiry deletes a data file only when *no*
retained snapshot still references it. Silver's five surviving snapshots between them
referenced all 93 of its data files, so nothing became unreachable and only manifest
lists went. Bronze's did not reference 78 files that earlier rewrites had already
replaced, so those were collectable. The same command, the same day, two completely
different outcomes — which is why "expire_snapshots frees disk" is not a claim worth
making without a number attached.

`remove_orphan_files` removed nothing, on both tables. That is the correct result and
the next section is why.

## Where the disk actually went: three counts, three answers

Same table, same moment, three questions that sound identical:

```sql
-- 1. what the current snapshot references
SELECT count(*) AS files, round(sum(file_size_in_bytes) / 1048576.0, 1) AS mib
FROM lakehouse.bronze.recentchange_raw.files;

-- 2. what any retained snapshot references. DISTINCT in both places, and the size
-- sum needs a subquery: see the mismeasurement section below.
SELECT count(DISTINCT file_path) AS files,
       round((SELECT sum(size) FROM (SELECT DISTINCT file_path, file_size_in_bytes AS size
                                     FROM lakehouse.bronze.recentchange_raw.all_data_files))
             / 1048576.0, 1) AS mib
FROM lakehouse.bronze.recentchange_raw.all_data_files;
```

```bash
# 3. what is on disk
docker compose exec -T minio sh -c 'du -sm /data/lakehouse/warehouse/bronze/recentchange_raw/data'
```

| Question | bronze | silver.edits |
|---|---|---|
| files in the current snapshot | 1 file, 69.4 MiB | 1 file, 28.0 MiB |
| distinct files in any retained snapshot | 74 files, 139.4 MiB | 93 files, 56.6 MiB |
| Parquet objects on disk | 74 objects, 141 MiB | 93 objects, 58 MiB |

The arithmetic closes exactly, and that is the point:

```
bronze:  69.4 MiB (the compacted file)  +  70.0 MiB (the 73 files it replaced)  =  139.4 MiB
silver:  28.0 MiB (the compacted file)  +  28.6 MiB (the 92 files it replaced)  =   56.6 MiB
```

The 73 pre-compaction files are still on disk because four older snapshots still
reference them, and those four are retained by the count floor. So the table you
query is 69.4 MiB and the table you pay for is 141 MiB. **Time travel is not free;
it costs the difference between rows 1 and 2 of that table.** On a laptop that is 82
MiB and nobody cares. In a system with a 90-day retention policy it is the storage
bill.

It also explains the zero orphans: 74 objects on disk against 74 distinct referenced
paths means every byte in the bucket is accounted for by some snapshot. There is
nothing orphaned to remove, so a green `remove_orphan_files` reporting
`orphan_files_removed=0` is the pipeline being correct, not the procedure being
broken. ADR-0024 covers the one flag that decides whether it can see the files at
all.

Metadata is a rounding error at this scale, but not zero: bronze holds 66 metadata
objects in 2 MiB and silver 74 in 2 MiB. `silver.quarantine` is the clearest view of
what a commit costs, because it has never held a single row and still has 5 snapshots
and 26 metadata objects — a `MERGE` that inserts nothing commits anyway. Its
`metadata.json` files are numbered `00004` to `00024`, exactly 21 of them, which is
`write.metadata.previous-versions-max = 20` plus the current one: proof that the
cleanup property is doing its job, since without it the numbering would start at
`00000` and never stop. The whole bucket is 202 MiB for 896,120 rows.

## Four ways to mismeasure this, all of which I did first

**`all_data_files` double-counts.** It has one row per manifest *entry*, so a file
referenced by six snapshots appears six times. `count(*)` on bronze returns a number
in the hundreds; `count(DISTINCT file_path)` returns 74. Every query above uses
`DISTINCT` for that reason, including the size sum, which needs
`SELECT DISTINCT file_path, file_size_in_bytes` in a subquery rather than
`sum(DISTINCT file_size_in_bytes)` — two files of identical size are two files.

**`du -sm A B C` deduplicates.** Passing a parent directory alongside its own
children makes the parent look nearly empty, because `du` counts each inode once and
the children were counted first. Measure siblings, never a directory and its
descendant in one invocation.

**MinIO stores each object as a directory.** `/data/lakehouse/warehouse/.../data/`
contains one directory per *partition*, and each of those contains one directory per
object, named `00000-123-xxxx.parquet`, holding the parts and an `xl.meta`. So
`ls -1 .../data | wc -l` counts partitions and not files, and the object count needs
`ls -1d .../data/*/*.parquet`. The 141 MiB versus 139.4 MiB gap is those directory
entries plus block rounding on 74 objects.

**The MinIO image has no `find` and no `grep`.** It is a distroless-style image with
a shell, `ls` and `du`. Anything more has to happen on the host over a pipe, which is
why the commands above look the way they do.

## Limitations

- One day of one laptop's data. The mechanisms — file count, manifest count, what
  expiry can and cannot collect — transfer. The absolute sizes do not, and
  `make maintain`'s own docstring says so.
- `rewrite_data_files` here always has one partition to work on, so it never
  demonstrates the interesting case: a rewrite that has to choose which of many
  partitions is worth the cost. `partial-progress.enabled` and bin-packing versus
  sorting strategies are untested by anything in this repository.
- Compaction is invoked manually. There is no scheduled maintenance asset, so the
  file count grows without bound if nobody runs it. Wiring it to the orchestration
  layer is the obvious next step and is not done.
- No equality-delete or position-delete files exist in either table, because nothing
  deletes rows. The `MERGE` in the silver job only inserts. So the delete-file side
  of Iceberg v2 — the part that makes compaction genuinely hard — is present in the
  format version and absent from the data.
