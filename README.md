# wikistream-lakehouse

A real-time lakehouse: Wikipedia's live edit stream into Kafka, Spark Structured
Streaming into Apache Iceberg, dbt marts over Trino, orchestrated by Dagster —
running end to end on a laptop.

## What this is

Wikimedia publishes every edit made across all its wikis as a public
Server-Sent-Events firehose. This repository consumes that firehose and turns it
into a queryable lakehouse on a single machine: a Python producer publishes each
event verbatim to Kafka, Spark Structured Streaming lands it in Apache Iceberg
tables on MinIO, dbt builds analytics marts over Trino, and Dagster models the
whole thing as assets with checks.

It exists to demonstrate the parts of data engineering that are hard to get
right rather than hard to wire up: exactly-once delivery into a table format
that has no primary keys, late-arriving events, restart safety under a hard
kill, schema evolution, and the small-files problem that streaming into a
lakehouse creates.

## Quickstart

PENDING-MEASUREMENT

## Architecture

PENDING-MEASUREMENT

## The problem and the constraints

PENDING-MEASUREMENT

## Design decisions

PENDING-MEASUREMENT

## Data contracts

PENDING-MEASUREMENT

## Correctness

PENDING-MEASUREMENT

## Testing and CI

PENDING-MEASUREMENT

## Operations

PENDING-MEASUREMENT

## Performance and cost

PENDING-MEASUREMENT

## What I would do differently

PENDING-MEASUREMENT

## Roadmap

PENDING-MEASUREMENT

## License

Apache License 2.0. See [LICENSE](LICENSE).

## Author

William Ankan Sarkar — [GitHub](https://github.com/william-sarkar)
