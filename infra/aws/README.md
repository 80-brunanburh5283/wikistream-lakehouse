# AWS reference architecture — a design artefact, never applied

**Read this first.** This Terraform module has never been applied. No AWS account
has ever seen it. There is no state file, no backend, no credentials in this
repository and no `terraform plan` in its history against real AWS credentials.
What CI does with it is `terraform fmt -check`, `terraform init -backend=false`,
`terraform validate` and `trivy config` — the checks that need no account. It is
here because the pipeline in this repository runs on one laptop for nothing, and
the question "what would this cost and look like as managed infrastructure" has a
specific, checkable answer that is worth writing down.

If you want to run something, run `make up` in the repository root. That works.
This does not, by design.

## What it would create

One module call per container in `docker-compose.yml`:

| Local | AWS | Module |
|---|---|---|
| Kafka, single-broker KRaft | MSK Serverless, SASL/IAM only | `modules/msk` |
| MinIO | Two S3 buckets, lifecycle rules | `modules/s3` |
| Iceberg REST catalog | Glue Data Catalog, one database per namespace | `modules/glue` |
| Spark Structured Streaming | EMR Serverless application | `modules/emr-serverless` |
| Trino | Athena — no infrastructure, see below | — |
| Dagster | nothing — see below | — |
| — | Two IAM roles, no users, no keys | `modules/iam` |
| — | Two security groups, no internet egress | `modules/network` |

`main.tf` is written to be read as that table: the module calls are in the order
data flows through them, and each module's header comment argues for the service
it picked over the alternative it rejected.

## What it deliberately does not create

**A VPC.** MSK Serverless and EMR Serverless both need private subnets, and every
account that would host this already has a network with an owner who is not this
module. `vpc_id` and `private_subnet_ids` are required inputs. See ADR-0035.

**VPC endpoints.** The security groups assume the account provides a gateway
endpoint for S3 and interface endpoints for Glue and STS, because the workers have
no route to the internet at all — there is no `0.0.0.0/0` egress rule anywhere in
`modules/network`. A missing endpoint looks like a Spark job that hangs on
`sts:AssumeRole` with no error, so it is worth checking before blaming the code.

**A GitHub OIDC provider.** There can be exactly one per URL per account and any
account running GitHub Actions already has it, so `modules/iam` references the
provider ARN it would have rather than creating a second one that fails.

**An Athena workgroup.** Athena is Trino, serverless, and the whole substitution is
a workgroup name and a results prefix. The IAM policy already scopes to both. What
changes in the dbt project is the adapter and four profile lines, which
`docs/architecture.md` sets out.

**Anything for Dagster.** There is no managed Dagster, so the honest options are a
paid hosted product this repository will not require, or a container platform —
which is a second infrastructure story with its own failure modes. `k8s/` is where
that goes.

**A Kafka topic.** MSK Serverless exposes no topic administration API, so
`wiki.recentchange` is created by a client. A `null_resource` running the Kafka CLI
would put the topic into Terraform state without putting it under Terraform's
control, which is worse than leaving it out.

## How it is checked

```bash
make infra-validate      # fmt -check, init -backend=false, validate
make infra-scan          # trivy config, fails on any finding
```

Both run in `.github/workflows/infra.yml` on every push. The workflow has no AWS
credentials configured and no `id-token` permission, so it could not authenticate
to AWS even if a step tried.

`trivy config` exits non-zero on any finding, and there are two suppressions in the
tree, both inline next to the code with the reason on the same line:

| Rule | Where | Why |
|---|---|---|
| `AVD-AWS-0132` — use a customer-managed KMS key | both buckets | SSE-KMS bills a KMS request per object write and per read. This table commits every 30 seconds in small files, and holds public Wikimedia data. The arithmetic is below. |
| `AVD-AWS-0089` — enable access logging | log bucket | A log bucket that logs its own access logs its own log writes, for ever. |

An earlier version of `modules/s3` created the public access blocks, encryption and
versioning with a `for_each` over a map of bucket ids. It was shorter and Trivy
reported both buckets as having none of the three, because it cannot follow
`bucket = each.value` back to the resource it names. The controls are written out
per bucket now. A control a static analyser cannot see is a control the next
reviewer has to verify by hand.

## What it would cost

Two kinds of number below, and the difference matters:

- **Prices** come from the AWS Price List bulk API for `eu-central-1`, which is the
  same data the pricing pages render. Each offer file carries its own publication
  date and they are listed under the table. Nothing here is from memory.
- **Volumes** come from `docs/throughput.md`, measured on 2026-09-17 against the
  live stream: **51.4 events/s**, **1,368 bytes** per event uncompressed, **316
  bytes** per event on the broker after zstd. That is 133.2 M events, 182 GB
  uncompressed and 42.1 GB compressed per 30-day month.

Everything else is an assumption, and each one is named in the line it affects.
Assumed: 730 hours a month, 7-day Kafka retention, two consumers of the topic,
30-second micro-batches on two streaming tables, four S3 PUTs per Iceberg commit,
and a dbt build every 15 minutes running 7 models and 61 tests.

| Line | Arithmetic | USD/month |
|---|---|---|
| MSK Serverless cluster | 730 h × $0.90 | 657.00 |
| MSK partitions | 3 × 730 h × $0.0018 | 3.94 |
| MSK data in | 42.1 GB × $0.12 | 5.05 |
| MSK data out (two consumers) | 84.2 GB × $0.06 | 5.05 |
| MSK storage (7-day retention) | 9.8 GB × $0.119 | 1.17 |
| EMR Serverless vCPU | 12 vCPU × 730 h × $0.060528 | 530.23 |
| EMR Serverless memory | 48 GB × 730 h × $0.006643 | 232.77 |
| S3 storage, first month | 84 GB × $0.0245 | 2.06 |
| S3 PUT | 691,200 × $0.0054/1,000 | 3.73 |
| S3 GET | 3.92 M × $0.0043/10,000 | 1.68 |
| Glue Data Catalog | 737 k requests, free tier is 1 M | 0.00 |
| Athena, floor | 195,840 queries × 10 MB minimum = 1.96 TB × $5.00 | 9.79 |
| **Total** | | **≈ 1,452** |

The EMR line assumes both streaming queries hold pre-initialised capacity
continuously — one driver and two executors each, 2 vCPU and 8 GB per worker,
which is what the local stack runs. Graviton workers would take that $763.00 to
$610.50 at the same shape. The Athena line is a floor, not an estimate: at 195,840
queries a month the 10 MB per-query minimum dominates, so the *test suite* costs
more than the models do, and a real figure needs bytes scanned that this
repository cannot measure without an account.

S3 storage is an upper bound rather than a measurement. It uses the compressed byte
rate measured on the Kafka broker; Iceberg's Parquet with zstd is columnar and will
be smaller on this data than JSON with zstd, so the real number is lower.

### The conclusion, which is not the flattering one

**$1,420 of the $1,452 — 98% — is cluster-hours and pre-initialised capacity.**
Those two lines are identical at 5 events a second and at 500. The data itself —
storage, requests, catalog, queries — costs under $20 a month.

So this module is a faithful translation of the local stack, and a faithful
translation is the wrong thing to deploy at 51 events per second. What would
actually go to production at this volume, with prices from the same offer files:

| Instead of | Use | USD/month | Gives up |
|---|---|---|---|
| MSK Serverless, $672 | Kinesis Data Streams on-demand: 730 h × $0.048 + 42.1 GB × $0.096 + 84.2 GB × $0.048 | ≈ 43 | The Kafka API, so the producer and both Spark readers change |
| MSK Serverless, $672 | MSK provisioned, 2 × kafka.t3.small: 2 × 730 h × $0.0526 + 40 GB × $0.119 | ≈ 82 | Broker sizing and storage autoscaling become yours |
| EMR Serverless streaming, $763 | The same job on a 15-minute schedule, ~3 minutes a run | ≈ 76 | Streaming. Latency goes from seconds to a quarter of an hour |

Roughly $140 a month for the batch-shaped version, an order of magnitude less. The
streaming architecture starts to make sense somewhere around 10,000 events a
second, where the fixed costs amortise against volume and the latency has someone
waiting on it. At 51 events a second it is a demonstration, and this document is
the part of the demonstration that says so.

### Price sources

All from `https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/<service>/current/eu-central-1/index.json`,
read on 2026-09-17. Each file publishes its own date:

| Service | Offer file | Published |
|---|---|---|
| MSK | `AmazonMSK` | 2026-09-11 |
| EMR Serverless | `ElasticMapReduce` | 2026-09-11 |
| S3 | `AmazonS3` | 2026-09-16 |
| Glue | `AWSGlue` | 2026-09-11 |
| Athena | `AmazonAthena` | 2026-09-11 |
| Kinesis | `AmazonKinesis` | 2026-09-11 |

Athena's 10 MB per-query minimum is not in the offer file; it is stated on
<https://aws.amazon.com/athena/pricing/>, read on 2026-09-17: billed "rounded up to
the nearest megabyte with a 10 megabyte minimum per query". Glue's 1 M
request-per-month free tier is in the offer file as a `Global-Catalog-Request`
dimension priced at zero up to 1,000,000.

## If you were going to apply this

You are not, and neither am I, but the honest list of what is missing is short and
naming it is more useful than pretending the module is complete:

- **State.** There is no backend block. An S3 backend with `use_lockfile = true`
  is what it would be; the reason it is absent rather than commented out is that a
  commented-out backend is one keystroke from being real.
- **A `terraform plan` against an account.** Every argument here is checked by the
  provider's schema, which catches a misspelled attribute and cannot catch a
  service quota, an IAM boundary or a subnet with no route.
- **The topic, and the job submission.** Creating `wiki.recentchange` and running
  `aws emr-serverless start-job-run` are steps this module deliberately leaves to a
  client. They are not written anywhere in this repository for AWS.
- **The Athena workgroup and the dbt profile.** Sketched in
  `docs/architecture.md`, not implemented.
