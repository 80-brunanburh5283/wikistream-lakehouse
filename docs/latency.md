# Source lag, and the watermark this measurement talked me out of

How stale is an event by the time this pipeline sees it? I measured it to size a
watermark, and the answer plus one experiment is why the silver stream has no
watermark at all. The measurement is still here because "how late is late" is the
question behind the quarantine's future-timestamp rule, the `late_by_seconds`
column, and how much history a `MERGE` has to look through.

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
None of those are things this pipeline controls, which is what makes it a
measurement of the source rather than of the pipeline.

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
last decimal place is not.** For deciding whether lateness is a matter of seconds
or of minutes, that precision is far more than enough.

## The distribution above is not the one a watermark has to survive

A watermark has to be sized against the worst arrival, not the typical one, and
none of the events in that sample were the interesting case. The interesting cases
are all recoveries:

- **The Spark job is stopped and restarted.** Kafka retains 24 hours, so on restart
  the job reads a backlog whose event times are as old as the outage. Against a
  30-second watermark, everything more than 30 seconds behind the newest event in
  the batch is late.
- **The producer reconnects.** One reconnect was observed during this 180-second
  measurement run — roughly one per three minutes of streaming — and each reconnect
  replays from `Last-Event-ID`, so it delivers events already seen and events whose
  timestamps precede the newest one already processed.
- **MediaWiki backfills.** Some `categorize` events are emitted as a consequence of
  an earlier edit rather than at the moment of the change.

So the number to size against is "how long might this pipeline have been down",
which is 24 hours before Kafka's retention makes the question moot — not 1.32
seconds. A watermark that covers 24 hours holds 24 hours of keys in state, which is
the entire cost the watermark was supposed to avoid.

## What the watermark would have done to the late rows

Measured, not argued, in `tests/spark/test_watermark_would_drop_data.py`:

```bash
uv run pytest -m spark -k watermark
```

Each design is given the same two events — one on time, one 30 minutes behind it —
one micro-batch each, and the table below is what came out the far side.

| design under a 10-minute watermark | the row 30 minutes late |
|---|---|
| `dropDuplicatesWithinWatermark("event_id")` | **silently dropped** |
| `dropDuplicates("event_id")` | **silently dropped** |
| no stateful operator at all | kept — a watermark changes nothing on its own |
| no watermark, `MERGE INTO` against the table (the design in use) | kept |

"Silently" is the operative word: no exception, no `_corrupt_record`, no quarantine
row, no metric that names it. `numOutputRows` is simply one lower than
`numInputRows`, and a reconnecting wiki's edits would be gone with nothing in the
pipeline saying so.

The cliff has a position, and the test pins that too: a row *five* minutes late comes
through `dropDuplicatesWithinWatermark` untouched. The objection to the rejected
design is where its edge falls, not that watermarks discard data indiscriminately.

So the silver stream declares no watermark. Deduplication is the table's job —
`MERGE INTO ... WHEN NOT MATCHED THEN INSERT` looks each key up in `silver.edits`
itself, so the dedup window is the table's whole history and no arrival is too late
to be recognised as a duplicate. ADR-0019 records the trade in full; the cost is
that the `MERGE` reads the target table on every batch, so write amplification grows
with table size instead of staying flat.

## Limitations of this measurement

- One 180-second sample from one location on one day. Wikimedia's traffic has a
  strong daily cycle; this was taken at roughly 09:45 local time (03:45 UTC),
  which is not the daily peak.
- Throughput of 40.2 events/second is the unfiltered `recentchange` rate,
  including the 52% of events that are `categorize` rather than human edits.
- The lag distribution says nothing about lag *inside* this pipeline. That is a
  separate number and is not claimed here.
- It also says nothing about the reconnect tail, which is the case that decided the
  design. No sample in this window was more than 2.74 s late, so the table above is
  evidence about a mechanism, not about how often the mechanism matters here.

Re-run the command at the top of this page to get current figures. Nothing in the
repository reads the values above: no watermark setting exists any more, and the two
places that used to cite one — `src/wikistream/config.py` and `.env.example` — now
say why there is nothing to configure.
