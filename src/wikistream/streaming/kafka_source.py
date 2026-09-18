"""The Kafka source, configured once for every streaming job that reads the topic.

Bronze and silver both consume `wiki.recentchange`, and they must consume it with
the same semantics — the same starting position, the same behaviour when offsets
have expired, the same bound on batch size. Two copies of these options is two
places for them to drift apart, and the drift would show up as one table having
data the other does not.

Each job still gets its own checkpoint and therefore its own independent position
in the topic. That is the point of reading Kafka twice rather than reading bronze:
silver's latency does not depend on bronze's, and either can be rebuilt without
the other. The cost is that the topic is read twice, which at this volume is
nothing and at real volume would be the first thing to change.

## Offsets, and the one flag that looks reckless

`startingOffsets=earliest` applies only on the very first run of a checkpoint.
Afterwards the checkpoint's offset log wins and the option is ignored — which is
the whole mechanism behind restart safety, and also why deleting a checkpoint
directory silently re-reads the topic from the beginning.

`failOnDataLoss=false` deserves its own paragraph, because it is the kind of flag
a reviewer is right to be suspicious of. It tells Spark to continue when the
offsets it recorded no longer exist on the broker. Here the topic's retention is
24 hours, so a laptop that is shut for two days *will* come back to a checkpoint
pointing at expired offsets, and the alternative behaviour — refusing to start —
turns "I closed my laptop" into a manual checkpoint deletion. The cost is real:
data that expired while the job was down is skipped rather than reported, and
nothing in the pipeline can distinguish that from a quiet period. That is
acceptable here because bronze's guarantee is "everything Kafka still had", not
"everything Wikimedia ever sent" — and it would not be acceptable in a system
where Kafka were the system of record. ADR-0016.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from wikistream.config import Settings

#: Records per micro-batch, per query. Without a bound, the first batch after a
#: long outage tries to read everything Kafka retained at once and an 11 GB box
#: meets an executor OOM instead of catching up steadily. 20,000 is about six
#: minutes of stream at the measured 51 events/s.
MAX_OFFSETS_PER_TRIGGER = 20_000


def read_kafka(
    spark: SparkSession,
    settings: Settings,
    topic: str | None = None,
    *,
    starting_offsets: str = "earliest",
) -> DataFrame:
    """Open the topic as a streaming source. Returns Kafka's own columns, untransformed.

    The caller supplies the checkpoint, so two queries built from this function are
    independent readers rather than a shared consumer group.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", topic or settings.kafka_topic)
        .option("startingOffsets", starting_offsets)
        # See the module docstring: a deliberate trade, not an oversight.
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", str(MAX_OFFSETS_PER_TRIGGER))
        # Kafka consumer group management is Spark's, not ours: it commits offsets
        # to the checkpoint, not to Kafka. Naming the group prefix anyway makes
        # `kafka-consumer-groups.sh --describe` show something recognisable.
        .option("groupIdPrefix", settings.kafka_consumer_group)
        .load()
    )
