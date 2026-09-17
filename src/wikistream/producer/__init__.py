"""Wikimedia SSE in, Kafka out.

Three modules, split along the line where the failure modes differ:

* `kafka_sink` owns delivery — idempotence, keying, buffering, and the counters
  that say whether records actually landed.
* `heartbeat` owns liveness, because "the container is running" and "the loop is
  turning" are different claims and only the second one matters.
* `__main__` owns the run: signals, bounds, and the shutdown flush.
"""
