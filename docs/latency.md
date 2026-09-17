# Source lag, and where the watermark number comes from

The silver stream carries a 10-minute watermark. This page is why, because a
watermark chosen by taste is a data-loss setting chosen by taste.

## What was measured

```bash
uv run python scripts/measure_source_lag.py --seconds 180
```

For every event, the script records

```
lag = (local wall-clock time the event was received) - (the event's own meta.dt)
```

That difference contains MediaWiki's internal propagation, Wikimedia's own Kafka
hop, the EventStreams HTTP hop, and the network path to the measuring machine.
None of those are things this pipeline controls, which is what makes the number
the right input to the watermark rather than a measurement of the pipeline
itself.

## Result

One 180-second sample, taken 2026-09-17 from a residential connection in Dhaka,
Bangladesh, against `stream.wikimedia.org`.

| statistic | lag |
|---|---|
| samples | 7,234 events |
| throughput | 40.2 events/second |
| min | −0.97 s |
| median | −0.36 s |
| mean | −0.34 s |
| p90 | 0.07 s |
| p99 | 1.32 s |
| p99.9 | 2.30 s |
| max | 2.74 s |

| lag bucket | events | share |
|---|---|---|
| < 1 s | 7,134 | 98.62% |
| 1–2 s | 44 | 0.61% |
| 2–5 s | 56 | 0.77% |
| > 5 s | 0 | 0% |

## The negative numbers are clock skew, and they are not hidden

82.49% of samples are negative: the event's own timestamp is in the future
relative to the measuring machine's clock. That means the local clock is *behind*
the source's by roughly a third of a second. WSL2 is a known offender here — its
clock can drift from the host's after a suspend — and no attempt is made to
correct for it, because the correction would be a guess about which of two clocks
is right.

The consequence is stated rather than papered over: **absolute lag values above
are accurate to about ±1 second. The shape of the distribution is reliable; the
last decimal place is not.** For choosing a watermark measured in minutes, that
precision is far more than enough.

## Why 10 minutes, when p99 is 1.32 seconds

The watermark is roughly 450× the measured p99. That looks absurd until you ask
what the watermark is actually protecting against, which is not steady-state
network jitter.

In steady state, lag is sub-second and a 30-second watermark would be generous.
The watermark exists for the case where the pipeline has *not* been in steady
state:

- The Spark job is stopped and restarted. Kafka retains 24 hours, so on restart
  the job reads a backlog whose event times are as old as the outage. With a
  30-second watermark, every event older than 30 seconds past the newest one in
  the batch is dropped as late — so a two-minute restart silently discards most
  of what it reads. That is the failure mode the restart-idempotency test would
  otherwise expose as missing rows.
- The producer reconnects. One reconnect was observed during this 180-second
  measurement run — roughly one per three minutes of streaming — and each
  reconnect replays from `Last-Event-ID`, delivering events already seen and
  events whose timestamps precede the newest already processed.
- MediaWiki backfills. Some `categorize` events are emitted as a consequence of
  an earlier edit rather than at the moment of the change.

So the watermark is sized for recovery, not for the network. Ten minutes covers a
restart of up to ten minutes without losing data.

## What it costs

State retention is the price of a wide watermark. At the measured 40 events/second,
ten minutes of watermark means Spark holds roughly

```
40 events/s x 600 s = 24,000 events
```

of deduplication state. That is small enough to be uninteresting on a laptop,
which is the honest reason the trade-off was easy to make here. On a stream two
orders of magnitude larger the calculation would go the other way, and the right
answer would be a tighter watermark plus explicit late-data handling into a
correction table.

## Limitations of this measurement

- One 180-second sample from one location on one day. Wikimedia's traffic has a
  strong daily cycle; this was taken at roughly 09:45 local time (03:45 UTC),
  which is not the daily peak.
- Throughput of 40.2 events/second is the unfiltered `recentchange` rate,
  including the 52% of events that are `categorize` rather than human edits.
- The lag distribution says nothing about lag *inside* this pipeline. That is a
  separate number and is not claimed here.

Re-run the command at the top of this page to get current figures; nothing in the
repository depends on the specific values above except this document and the
watermark default in `src/wikistream/config.py`.
