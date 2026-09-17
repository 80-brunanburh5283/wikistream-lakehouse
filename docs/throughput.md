# Ingest throughput, compression and partition balance

Three numbers in `src/wikistream/config.py` are defended here: the compression
codec, the partition count, and the size of the producer's send buffer. Each was
measured on this machine against the live stream, and each command that produced a
figure is named next to it.

## What was measured, and how to repeat it

```bash
make up-core                 # Kafka and MinIO
make measure-throughput      # one bounded producer run, then read the broker
make measure-compression     # the same run once per codec
make explain-partitions      # no network: why the balance looks the way it does
```

`measure-throughput` compares two independent counts of the same data: the
producer's own `bytes_produced` counter, which is the sum of the UTF-8 payloads it
handed to librdkafka, against `kafka-log-dirs.sh`, which is what the broker has on
disk afterwards. Nothing is estimated and nothing is sampled.

## Result: one 5,000-frame run

Measured 2026-09-17 at 08:16 UTC, on WSL2 with 10 GB of RAM and 8 vCPUs, against
`stream.wikimedia.org` from a residential connection in Dhaka.

| Metric | Value |
|---|---|
| Frames produced | 5,000 |
| Frames acknowledged by the broker | 5,000 |
| Delivery failures | 0 |
| Wall clock | 97.3 s |
| Throughput | 51.4 events/s |
| Uncompressed payload | 6,841,047 bytes |
| Stored on the broker | 1,578,825 bytes |
| Compression ratio (zstd) | 4.33x |
| Stored bytes per record | 316 |
| Mean payload per record | 1,368 bytes |

Throughput here is the stream's rate, not the pipeline's capacity. The producer is
never the bottleneck at this volume: `backpressure_waits` stayed at 0 for the whole
run, meaning the local send queue never filled, and the queue depth sat at 1 record
except for one 21-record blip. The rate is what Wikimedia was emitting, and it moves
with the time of day — `docs/latency.md` recorded 40.2 events/s at 03:45 UTC against
51.4 here at 08:16.

### What this costs in disk

At the measured rate and record size, a full day of Kafka retention is

```
51.4 events/s x 86,400 s x 316 bytes = 1.40 GB/day compressed
                            (1,368 bytes = 6.08 GB/day uncompressed)
```

That is the whole reason `retention.ms` is 24 hours rather than a week: a week of
this is 10 GB of Kafka on a laptop that also has to run Spark, and Iceberg — not
Kafka — is the audit trail that is supposed to keep data for a long time.

## Compression: what the codecs actually did

Five runs of 2,000 frames each, one per codec, same `linger.ms` and `batch.size`,
each into its own topic. Ratio is uncompressed payload divided by bytes on disk.

| Codec | Frames | Uncompressed bytes | Stored bytes | Ratio | Stored per record |
|---|---|---|---|---|---|
| none | 2,000 | 2,728,243 | 2,802,713 | 0.97x | 1,401 |
| gzip | 2,000 | 2,729,139 | 624,563 | **4.37x** | 312 |
| zstd | 2,000 | 2,723,681 | 651,258 | 4.18x | 326 |
| snappy | 2,000 | 2,772,961 | 852,925 | 3.25x | 426 |
| lz4 | 2,000 | 2,692,261 | 843,033 | 3.19x | 422 |

Two things in that table are worth stopping on.

**`none` measures 0.97x, not 1.00x.** The stored figure includes Kafka's
record-batch framing and the message keys, which the payload counter does not. So
every ratio in the table is understated by about 3%, uniformly. That is deliberate:
disk is what has to be paid for, so disk is what is counted.

**gzip beat zstd.** By 4.5%, which is inside the run-to-run variance — zstd measured
4.18x here and 4.33x in the 5,000-frame run above, on different samples of the same
stream. The honest conclusion is that gzip and zstd are indistinguishable on ratio
for this data, and both are a clear 30% better than lz4 and snappy.

So the default is not a ratio win, and the earlier version of this repository's
comment claiming zstd had "a better ratio" was wrong. The reason zstd is still the
default:

- The choice between gzip and zstd is a CPU-per-byte trade, and **this project did
  not measure CPU.** At 51 events/s neither codec is remotely close to being the
  constraint — the producer is idle waiting on the network. If this ran at 50,000
  events/s, codec CPU would be the first thing to measure and the answer might
  change.
- What is being optimised at this volume is not bytes or cycles but *not having to
  revisit the decision later*. zstd has a tunable level, so the same setting scales
  from "cheap" to "small" without changing codecs; gzip does not.

lz4 and snappy are the right answer for a latency-sensitive topic, where the point
of compression is to shrink the network hop rather than the disk. This topic has a
30-second micro-batch trigger downstream, so a millisecond of codec latency is
invisible, while snappy's 426 bytes per record against zstd's 326 is 31% more disk
for every day of retention, and that is not.

## Partition balance: worse than the obvious estimate

The producer keys every record on `meta.domain`, so all edits to one wiki land on
one partition and stay ordered relative to each other. The cost is balance. The
estimate in the original decision record was the busiest wiki's share of traffic,
31.8%. The measured busiest *partition* was much worse:

| Partition | Records | Share |
|---|---|---|
| 0 | 749 | 15.0% |
| 1 | 1,338 | 26.8% |
| 2 | 2,913 | **58.3%** |

`make explain-partitions` shows why, offline, from the captured fixture.
librdkafka's default partitioner maps a key with `crc32(key) % partition_count`,
and with three partitions the two busiest wikis collide:

| Wiki | Share of the capture | Partition (of 3) |
|---|---|---|
| `commons.wikimedia.org` | 31.8% | 2 |
| `id.wikipedia.org` | 21.8% | 2 |
| `www.wikidata.org` | 13.6% | 1 |
| `ar.wiktionary.org` | 13.0% | 0 |
| `en.wikipedia.org` | 7.4% | 0 |

31.8% + 21.8% + the smaller wikis that also land there predicts 61.2% on partition
2, against 58.3% measured on a different sample hours later. The mechanism is
confirmed; it is not a hash-quality problem but two heavy keys landing together.

### Would more partitions help?

Partly, and then not at all:

| Partitions | Busiest partition | Idle partitions | Perfect balance would be |
|---|---|---|---|
| 1 | 100.0% | 0 | 100.0% |
| 3 | 61.2% | 0 | 33.3% |
| 6 | 35.2% | 0 | 16.7% |
| 12 | 32.8% | 2 | 8.3% |
| 24 | 32.4% | 7 | 4.2% |

The floor is 31.8%, because `commons.wikimedia.org` is that share of traffic and a
single key never splits across partitions. Six partitions gets within 4 points of
the floor; twelve buys 2.4 points of balance and two partitions that receive
nothing.

Three is kept anyway, and that is a deliberate laptop-shaped decision rather than
the right answer for a cluster:

- Spark reads one Kafka partition per task. With 6 partitions and 2 executor cores,
  half the tasks queue, and the skew stops being the constraint before the
  parallelism does.
- Every partition is a directory of segment files and a slice of consumer state. On
  a 12 GB box running Kafka, Spark, Trino and Dagster together, twelve of those to
  save 2.4 points of balance is the wrong trade.
- The skew is *visible* rather than harmful here. Nothing in the pipeline reads a
  single partition, silver's MERGE is keyed on the event id and not the partition,
  and 58% of 51 events/s is 30 events/s on one partition.

If this were a real deployment at real volume the answer would be different, and
the honest version of "different" is not "more partitions": it is a composite key —
`meta.domain` plus a bucket of `meta.id` — for the handful of wikis large enough to
need it, accepting that per-wiki ordering is then per-bucket ordering. That is a
change to what the pipeline guarantees, not a topic setting, which is why it is not
made here on the strength of one laptop measurement.

## The send buffer

`queue.buffering.max.kbytes` is 64 MB, against librdkafka's default of 1 GB. At the
measured 1,368 bytes per record, 64 MB is roughly

```
67,108,864 / 1,368 = 49,000 records = 16 minutes of stream at 51 events/s
```

of outage absorbed before `send` starts blocking and logging. The default would
absorb four hours and then be OOM-killed by the kernel on a laptop, which converts a
recoverable broker outage into a lost process and an unexplained restart.

## Limitations

- One machine, one location, one time of day. Every figure moves with the stream's
  daily cycle; the ratios between codecs and partitions do not.
- The codec comparison gives each codec a different 2,000-frame sample, so its third
  digit is noise. Ordering and the ~30% gap between the two groups are reliable.
- CPU cost per codec is not measured. See the note above; this is the most
  significant gap in this page.
- These are ingest numbers only. End-to-end latency through Spark and Iceberg is a
  different measurement and is not claimed here.
