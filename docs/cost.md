# Cost

Two questions, and they have very different answers.

**What does this cost to run locally?** Nothing, in money. That is a constraint the
project was built under, not an accident: everything is a container, the data source
is a free public stream, and no command in the repository creates a cloud resource.
The real local cost is memory, and it is measured below.

**What would the same design cost as managed AWS services?** About **$1,240 a
month** at the volume this pipeline actually moves — roughly **$9.29 per million
events**, of which keeping the data, requests included, is nine *cents*. Every figure
behind that is
computed from AWS's public price list with no account, no credentials and nothing
deployed. `infra/aws/` describes the architecture that number prices, and
[its README](../infra/aws/README.md) opens by saying it has never been applied.

The interesting part is not the total. It is that 93% of it buys *availability* —
two services that must be running whether or not an event arrives — and 0.03% buys
storage of the data itself.

## What it costs to run locally

| Resource | Measured | How |
|---|---|---|
| Peak memory, whole stack | 6,974 MiB summed peaks | `make measure-resources` |
| Peak memory, largest single container | Spark, 2,823 MiB | same |
| Next two | Trino 1,668 MiB, Kafka 761 MiB | same |
| Images built here | 4.63 GB — Spark 2.37, Dagster 1.14, dbt 0.87, producer 0.25 | `docker images` |
| Images on disk, all nine | ~8.6 GB of layers — the four above plus Trino, Kafka, the Iceberg fixture, MinIO and `mc` | `docker system df -v`, summing UNIQUE SIZE and counting each shared base once |
| First build, no layer cache | 22m50s for all four | `docker compose build --no-cache`, timed |
| Iceberg table data | 280 MiB after 1,218,424 events | `make table-stats`, 2026-09-18 |
| Kafka log, steady state | 1.40 GB at 24 h retention | [throughput.md](throughput.md) |
| Inbound bandwidth | ~6 GB/day of JSON, continuous | 1,368 B/event × 51.4 events/s |
| Container log output | 28.8 MiB/day across seven services | `docker compose logs --since 30m \| wc -c` |
| Money | $0.00 | — |

The sum of peaks is an upper bound, not a reading: no two containers are guaranteed
to peak in the same second. What it establishes is the one number a reviewer needs
before cloning — about 7 GB of memory has to be available to Docker — and
`scripts/bootstrap.sh` checks for it before the first image pull: it passes at 10 GB,
warns between 6 and 10 that the full profile will not fit, and fails below 6. The
sample above
is 23 readings over five minutes with both streams and the producer running and no
query in flight; a `make dbt-build` pushes Trino above its 1,668 MiB resting heap,
which is why `docker-compose.yml` caps that container at 2 GB rather than letting it
find out how much the box has.

The two costs that are real but not billed to this project are electricity, which
was not measured, and 6 GB a day of inbound bandwidth, which matters on a metered
connection and is worth knowing before leaving `make ingest` running for a week.

CI adds nothing: the workflows run on GitHub-hosted `ubuntu-latest` runners with no
credentials and no cloud calls at all.

## Where the AWS prices come from

The bulk Price List API. It is public, unauthenticated, and returns exact strings
rather than the rounded figures on the pricing pages, which is why every price on
this page can be re-derived by a reader with `curl` and `jq` and no AWS account:

```bash
P=https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws

curl -s "$P/AmazonMSK/current/eu-central-1/index.json" | jq -r '
  .terms.OnDemand as $t
  | .products | to_entries[]
  | select(.value.attributes.usagetype | test("Serverless"))
  | .key as $sku
  | ($t[$sku] // {}) | to_entries[]?.value.priceDimensions | to_entries[]?.value
  | "\(.description) — \(.pricePerUnit.USD) per \(.unit)"' | sort -u
```

Substitute `ElasticMapReduce`, `AmazonS3`, `AWSGlue`, `AmazonAthena`, `awskms`,
`AmazonVPC` or `AmazonCloudWatch` for the offer code, and drop the `select` when
looking for something else. Each file carries its own `publicationDate`, which is
the date quoted in the table below — a price list is a dated document and quoting a
price without one is quoting nothing.

Region is **eu-central-1** (Frankfurt), which is `infra/aws/variables.tf`'s default.
Prices differ by region by 10–20%, so the arithmetic here travels and the totals do
not. Taxes, support plans and any new-account free tier are excluded.

## The unit prices used

| Service | Dimension | Price | Published |
|---|---|---|---|
| MSK Serverless | cluster | $0.90 / hour | 2026-09-11 |
| MSK Serverless | partition | $0.0018 / hour | 2026-09-11 |
| MSK Serverless | data in | $0.12 / GB | 2026-09-11 |
| MSK Serverless | data out | $0.06 / GB | 2026-09-11 |
| MSK Serverless | storage | $0.119 / GB-month | 2026-09-11 |
| EMR Serverless | vCPU, x86 | $0.060528 / hour | 2026-09-11 |
| EMR Serverless | memory, x86 | $0.006643 / GB-hour | 2026-09-11 |
| EMR Serverless | vCPU, ARM | $0.048425 / hour | 2026-09-11 |
| EMR Serverless | memory, ARM | $0.005317 / GB-hour | 2026-09-11 |
| EMR Serverless | ephemeral storage | $0.000132 / GB-hour | 2026-09-11 |
| S3 | Standard, first 50 TB | $0.0245 / GB-month | 2026-09-17 |
| S3 | Standard-IA | $0.0135 / GB-month | 2026-09-17 |
| S3 | PUT, COPY, POST, LIST | $0.0054 / 1,000 | 2026-09-17 |
| S3 | GET and all others | $0.0043 / 10,000 | 2026-09-17 |
| S3 | GET from Standard-IA | $0.01 / 10,000 | 2026-09-17 |
| S3 | transition to Standard-IA | $0.01 / 1,000 | 2026-09-17 |
| S3 | Standard-IA retrieval | $0.01 / GB | 2026-09-17 |
| Glue Data Catalog | requests | $1 / million, first million free | 2026-09-11 |
| Glue Data Catalog | objects stored | $1 / 100,000-month, first million free | 2026-09-11 |
| Athena | data scanned | $5.00 / TB | 2026-09-11 |
| KMS | customer-managed key version | $1 / month | 2026-09-11 |
| KMS | requests | $0.03 / 10,000 | 2026-09-11 |
| VPC | interface endpoint | $0.012 / hour | 2026-09-17 |
| VPC | interface endpoint data | $0.01 / GB | 2026-09-17 |
| CloudWatch Logs | ingestion, standard class | $0.63 / GB | 2026-09-15 |
| CloudWatch Logs | storage | $0.0324 / GB-month | 2026-09-15 |

Athena also bills a **10 MB minimum per query**, rounded up to the megabyte, and
nothing for DDL — [aws.amazon.com/athena/pricing](https://aws.amazon.com/athena/pricing/),
read 2026-09-18. That minimum turns out to matter more here than the $5, and it is
the one line in this table a reader is most likely to skip.

## The workload being priced

Every input is a measurement from this repository, not an estimate of a
hypothetical:

| Input | Value | Source |
|---|---|---|
| Ingest rate | 51.4 events/s | [throughput.md](throughput.md), 2026-09-17 |
| Bytes per record on the broker | 316 (zstd) | same |
| Bytes per record on the wire from Wikimedia | 1,368 | same |
| Kafka partitions, retention | 3, 24 hours | `src/wikistream/config.py` |
| Commits per stream per day | 2,880 | 30 s trigger; measured cadence 33.4 s |
| Data files added per commit | 3.99 | `snapshots` metadata table, both tables |
| Objects written per commit | 7 | 4 data files + manifest + manifest list + `metadata.json` |
| Bytes per event, bronze / silver | 166.6 / 74.2 | `make table-stats`, 1,218,424 rows |
| New table data per day | 1.07 GB | derived from the two rows above |
| dbt build cadence | every 15 minutes | `src/wikistream_dagster/schedules.py` |
| Statements per dbt build | 69 — 7 models, 62 tests | dbt manifest, `make dbt-parse` |
| Observation cadence | every 2 minutes | `schedules.py` |
| Month | 30 days, both streams continuous | — |

Two of those deserve a note. The commit rate is the *nominal* one: at a 30-second
trigger a stream commits 2,880 times a day, and the measured cadence over a
15-minute window was one commit every 33.4 seconds, so 2,880 overstates by about
10%. Overstating is the right direction for a cost model. And the 69 statements per
dbt build are what `dbt build` runs — models *and* tests — because that is what the
Dagster job does, with the tests as asset checks.

## The bill

| Line item | Arithmetic | $/month |
|---|---|---|
| MSK Serverless, cluster | 720 h × $0.90 | 648.00 |
| MSK Serverless, partitions | 3 × 720 h × $0.0018 | 3.89 |
| MSK Serverless, data in | 42 GB × $0.12 | 5.04 |
| MSK Serverless, data out | 84 GB (two consumers) × $0.06 | 5.04 |
| MSK Serverless, storage | 1.4 GB × $0.119 | 0.17 |
| EMR Serverless, held capacity | 5,760 vCPU-h + 23,040 GB-h, x86 | 501.70 |
| EMR Serverless, worker disk | ≤ 80 GB × 720 h × $0.000132 | 0.00–7.60 |
| Interface VPC endpoints | 3 services × 2 AZ × 720 h × $0.012 | 51.84 |
| Athena | 198,720 queries × 10 MB minimum = 1.99 TB | 9.94 |
| S3, PUT | 1.21M × $0.0054/1,000 | 6.53 |
| S3, GET | 12.5M × $0.0043/10,000 | 5.39 |
| CloudWatch Logs | 0.86 GB × $0.63 | 0.57 |
| S3, storage (first month) | ~16 GB-month average × $0.0245 | 0.39 |
| Glue Data Catalog | ~1.0M requests, against a 1M free allowance | 0.00–1.00 |
| **Total** | | **≈ 1,238** |

Sorted by size, that table says something the architecture diagram does not:

| What the money buys | $/month | Share |
|---|---|---|
| Being available — the MSK cluster fee and the held EMR workers | 1,149.70 | 92.8% |
| Being reachable privately — VPC interface endpoints | 51.84 | 4.2% |
| Moving the data — Kafka ingress and egress, S3 PUTs | 16.61 | 1.3% |
| Answering questions — Athena, and the S3 GETs behind it | 15.33 | 1.2% |
| Everything else — partition-hours, Kafka storage, logs | 4.63 | 0.4% |
| **Keeping the data** — S3 storage | **0.39** | **0.03%** |

At 133 million events a month, the whole bill is $9.29 per million events. Storage
and every request against it together are $0.09 of that. **This is not a data
pipeline whose cost is driven by data.** It is two always-on services with a
lakehouse attached as a rounding error, and the design lever that matters is
therefore never compression or file layout — it is how much of the time anything
needs to be running. The next two sections are that lever.

## EMR Serverless is not billed like serverless here

`modules/emr-serverless/main.tf` says this in prose and defers the arithmetic to
this page, so here it is.

EMR Serverless bills vCPU-hours and GB-hours for as long as workers are held. A
Structured Streaming query never finishes, so its workers are never released, and
the module additionally declares **pre-initialised capacity** — one driver and two
executors at 2 vCPU / 8 GB each — so that a restart does not wait two minutes for
provisioning while the Kafka retention clock keeps running. That capacity is billed
while the application is started, job or no job.

| Configuration | vCPU | Memory | $/month, x86 | $/month, ARM |
|---|---|---|---|---|
| The module's pre-initialised capacity | 6 | 24 GB | 376.27 | 301.07 |
| Both streaming queries (second driver on demand) | 8 | 32 GB | 501.70 | 401.43 |

Three things follow.

**The sizing is set by EMR's worker shapes, not by the workload.** The local Spark
container peaks at 2,801 MiB running *both* queries, and this configuration provides
32 GB. There is no 0.5 vCPU worker to buy; the smallest useful shape is far larger
than the job, and the bill reflects the shape.

**Graviton is a 20% discount for one attribute.** $0.048425 against $0.060528 per
vCPU-hour, $0.005317 against $0.006643 per GB-hour — $100 a month on this
configuration. The module does not set `architecture` on
`aws_emrserverless_application`, so it gets the `X86_64` default. That is not a
considered choice, it is an omission I noticed while writing this page, and the
reason I have not simply changed it is that nothing here has verified the Kafka and
Iceberg jars in the image are ARM-clean, and a design artefact that has never been
applied is a bad place to assert that they are.

**`auto_stop_configuration` never fires while the stream is healthy.** It is not a
cost control for this workload; it is a guard against paying for pre-initialised
capacity through a weekend after a restart that nobody noticed. The real cost
control is `maximum_capacity`, which is why `emr_max_cpu` defaults to 32 rather than
being left open.

## What freshness costs

The 30-second trigger is a choice, and it is the most expensive line in the design.
Holding it against the alternative — `Trigger.AvailableNow`, which starts, drains
everything available, and exits — gives the price of freshness directly.

| Shape | End-to-end lag | EMR $/month | MSK $/month | Total |
|---|---|---|---|---|
| Continuous, 30 s trigger (what this repo runs) | ~30 s | 501.70 | 662.14 | 1,163.84 |
| `AvailableNow` every 15 minutes | up to 15 min | 75.25 | 662.14 | 737.39 |
| `AvailableNow` hourly | up to 1 h | 18.81 | 662.14 | 680.95 |

The EMR figures assume three minutes of billed worker time per invocation — Spark
start-up plus a batch, with no pre-initialised capacity to pay for between runs —
and scale linearly with that assumption, which is the one number in this document
that is a guess rather than a measurement. Even generous, the conclusion holds:
**dropping from 30-second freshness to 15-minute freshness cuts the compute bill by
85%, and the total by 37%.**

The total falls by less than the compute because the queue does not move. $662 a
month is charged for the MSK cluster whether the pipeline reads it every 30 seconds
or once an hour, and no trigger tuning touches it. Which raises the honest question:

**At 51 events per second, is Kafka worth $662 a month?** What it buys here is
specific and real — 24 hours of replay, two independent consumers reading the same
records at their own pace, and the per-partition offsets that
`bronze_offsets_are_contiguous` uses to prove nothing was lost between the queue and
the table ([correctness.md](correctness.md)). Those are the load-bearing properties
of this design, not decoration. But at 15-minute freshness they can all be had by
writing raw frames to S3 and reading them as an Iceberg table, and the answer is
then that the queue is unnecessary. A streaming architecture is worth its
availability floor when the freshness requirement is measured in seconds. It is a
$662-a-month habit when it is not, and "we already have Kafka" is how that habit
survives review.

## S3: the requests cost more than the bytes

The small-files problem, expressed in dollars rather than in file counts.

Each commit writes 7 objects, two streams commit 2,880 times a day each, so the
lakehouse issues **1.21 million PUTs a month** and stores 32 GB. The PUTs cost
$6.53; the storage costs $0.39. Reads make it starker: an uncompacted `silver.edits`
had 826 data files, and a dbt build that scans it once per model, four times an hour,
is 11.9 million GETs a month.

| Cost | Uncompacted | After `make maintain` | Ratio |
|---|---|---|---|
| S3 GETs for the mart builds | $5.11 | $0.19 | 27× |
| Files scanned per model | 826 | ~30 (one per day-partition) | 27× |

Compaction is usually presented as a query-performance optimisation. It is also a
line item: at this commit rate it is worth more per month than the storage it
rewrites. And it is not free — [lakehouse.md](lakehouse.md) measured
`rewrite_data_files` moving 70 MiB to save 0.6 MiB of space, and the rewritten
inputs stay on disk until their snapshots expire, so **the table you pay for is
larger than the table you query**: 139.4 MiB of retained files behind a 69.4 MiB
current snapshot, measured. Time travel is a storage multiplier, and at 32 GB a
month nobody notices. At a 90-day retention policy it is the storage bill.

### When Standard-IA pays, and when it is a fee for nothing

The module transitions `warehouse/bronze/data/` to Standard-IA at 30 days
(`bronze_transition_days`). IA saves $0.011 per GB-month and charges $0.01 per 1,000
transitions, so the payback depends entirely on object size:

| Object | Size | IA saving | Transition cost | Payback |
|---|---|---|---|---|
| An uncompacted bronze data file | 240.4 KiB (measured average) | $0.0000025/month | $0.00001 | 4 months |
| A compacted daily file | 69.4 MiB (measured) | $0.00075/month | $0.00001 | 10 hours |

That is the same rule from two directions: **compact, then transition.** A lifecycle
rule pointed at a streaming table's raw output is a transition fee attached to
objects that may not survive four months, and AWS says as much — Standard-IA has a
30-day minimum storage duration and bills any object under 128 KB as 128 KB
([storage class documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html),
read 2026-09-18). `silver.edits` averaged 106.9 KiB per file before compaction,
which is below that floor: transitioning it would have billed 20% more bytes than it
stores, at a cheaper rate, and called it a saving. The module sets
`transition_default_minimum_object_size = "all_storage_classes_128K"` so that S3
skips those objects itself.

The same arithmetic is why the rule's `prefix` excludes `metadata/`. IA charges $0.01
per 10,000 GETs against Standard's $0.0043 — **2.3× more per read** — and manifests
are read by every query plan. Moving kilobyte-sized files that are read constantly
into a storage class that charges more per read is a saving in the storage column and
a loss everywhere else.

## Encryption: what SSE-KMS would have cost

`modules/s3/main.tf` sets `sse_algorithm = "AES256"` and suppresses the scanner
finding that wants `aws:kms`, with a comment promising the arithmetic here. SSE-KMS
with a customer-managed key calls KMS once per object written and once per object
read, at $0.03 per 10,000 requests, plus $1 per key version per month.

| | AES256 (SSE-S3) | SSE-KMS, no bucket key |
|---|---|---|
| Key | $0.00 | $1.00 |
| 1.21M object writes | $0.00 | $3.63 |
| 12.5M object reads | $0.00 | $37.63 |
| **Total** | **$0.00** | **$42.26** |

$42.26 a month to encrypt data whose storage costs $0.39 a month — **108× the cost
of the thing being protected** — for a table built from a public Wikimedia feed that
anyone can read at the source. The read side dominates, and it dominates precisely
because this is a streaming table read as thousands of small files: the KMS bill is a
function of file count, which is a function of the trigger interval, which has
nothing to do with security.

The decision is not "encryption is expensive, skip it". Both buckets are encrypted
at rest and a bucket policy denies unencrypted transport. What is being declined is a
*customer-managed key*, which buys a key policy, rotation and a CloudTrail record of
every decrypt — controls that are worth paying for when the data is confidential and
buy nothing when the data is a public feed.

If this table held anything private the answer flips, and the flip has a fix
attached: enable **S3 Bucket Keys**, which cache a bucket-level key so that KMS is
called per bucket-key lifetime instead of per object, and which AWS documents as
reducing KMS request costs "by up to 99 percent"
([bucket key documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-key.html),
read 2026-09-18). That is one Terraform attribute — `bucket_key_enabled = true` —
and it turns $42.26 into something near $1.40. A design that needs SSE-KMS and
does not set it is paying for a per-object call it could have avoided.

## Glue and Athena round to zero, but not for the reason you would guess

The Glue Data Catalog's free allowance — one million requests and one million objects
a month, both of which appear as $0 price dimensions in the price list itself —
covers this pipeline. Barely. Every Iceberg commit is a handful of catalog calls, and
at 5,760 commits a day plus an observation every two minutes plus 96 dbt builds, the
estimate lands within a few percent of a million requests a month. So the line item
is either $0 or $1, and no amount of care in the estimate decides which. What is
worth noticing is the sensitivity: halving the trigger interval to 15 seconds doubles
the catalog traffic. The number is small, and it is not small because the workload is
small — it is small because the free allowance happens to be the same order of
magnitude.

Athena is the same story with the opposite cause. The marts are tiny: a full scan of
`silver.edits` at the measured size is 86.2 MiB, which is $0.0004. But a `dbt build`
is 69 statements, 62 of them tests that scan almost nothing, and Athena bills a 10 MB
minimum per query. Four builds an hour is 198,720 queries a month, and 198,720
minimum charges is 1.99 TB of billable scan against a few gigabytes of actual reads —
$9.94 a month, **25 times the cost of storing everything the queries read.** The
lever here is not partitioning or file size, it is the test schedule: 62 assertions do
not need to run every fifteen minutes, and splitting model builds from test runs would
remove most of that line without removing a single test.

## What I would change if this had to run for real

In descending order of money saved:

1. **Question the freshness requirement first.** 30 seconds to 15 minutes is $426 a
   month, and it is a product decision that engineers routinely make by default.
2. **Then question the queue.** $662 a month is the floor under this design. If the
   answer to (1) is 15 minutes, Kafka is buying replay and multi-consumer fan-out
   that S3 can provide for a few dollars, and it should go.
3. **Switch EMR Serverless to ARM.** 20%, one attribute, after checking the jars.
4. **Run both queries in one job.** Two continuous job runs hold two drivers. One
   Spark session with two queries and `awaitAnyTermination` holds one — $125 a month
   for a change that makes the failure domains shared, which is a real trade and not
   a free win.
5. **Compact before transitioning, and split the dbt test schedule from the build
   schedule.** Both are single-figure sums here, and both grow linearly with the
   table while the storage bill grows with the bytes.

The generalisable lesson is the ratio at the top: 92.9% availability, 0.03% storage.
Streaming costs money for being ready, not for what it carries, so the first cost
question about any streaming pipeline is not "how much data?" but "how fresh, really,
and for how many hours a day?"

## What this page does not price

- **GCP.** [architecture.md](architecture.md) covers the portability reasoning
  without pretending there is a second implementation; there is no GCP price model
  here either.
- **Dagster.** There is no managed Dagster and `k8s/` has never run on a real
  cluster, so an orchestration line item would be invented. On EKS it would be the
  cluster fee plus a node, which is likely to exceed the EMR line.
- **The Spark drivers' log volume.** The 28.8 MiB/day measured locally covers seven
  containers and excludes the two streaming jobs, whose output goes to the terminal
  that launched them rather than to a container log. Spark at default INFO level is
  the easiest way to make CloudWatch Logs the third-largest line on this page:
  ingestion is $0.63/GB, so 1.6 GB of log costs a dollar, and a dollar stores 40 GB
  of Parquet for a month.
- **Cross-AZ data transfer, NAT, CloudTrail data events, and S3 access log storage.**
  There is no NAT gateway line item because the design needs none: the workers run in
  private subnets the account supplies, and their security groups have no
  `0.0.0.0/0` egress rule, so there is no route to the internet to bill for. NAT
  gateways are charged hourly *and* per gigabyte, and avoiding them is the largest
  saving in this design that came from a security decision rather than a cost one.
  Access logging is used instead of CloudTrail data events for the same double
  reason: it is the cheaper of the two and the only one that does not bill per
  request against a bucket that receives 1.2 million writes a month.
- **Anything observed.** Nothing in `infra/aws/` has ever been applied. These are
  published prices multiplied by measured volumes, which is a forecast, not an
  invoice. A real deployment finds line items no price list predicts, and the honest
  version of this page ends by saying so.
