"""Reconnect delay policy, isolated so it can be tested without a network.

Kept separate from the sources for two reasons: both sources need it, and the
interesting property — that a flapping upstream does not get hammered, and that a
recovered upstream is not punished with a minute of silence — is a property of
this arithmetic alone.
"""

from __future__ import annotations

import random
from collections.abc import Callable


class ExponentialBackoff:
    """Exponential delay with equal jitter and a cap.

    Jitter matters even for a single client. Without it, a client that loses a
    connection at the same moment as everyone else retries at the same moment as
    everyone else, and the upstream's recovery is met with a synchronised
    thundering herd. Wikimedia's firehose has many consumers.

    Equal jitter — half the delay fixed, half random — rather than full jitter,
    because full jitter can return a delay near zero on a late attempt and
    briefly reproduce the tight loop the backoff exists to prevent.
    """

    def __init__(
        self,
        initial_seconds: float,
        max_seconds: float,
        *,
        rng: Callable[[], float] = random.random,
    ) -> None:
        """Configure the schedule.

        Args:
            initial_seconds: Delay after the first failure.
            max_seconds: Ceiling on the un-jittered delay.
            rng: Returns a float in [0, 1). Injected so tests are deterministic.
        """
        if initial_seconds <= 0:
            msg = f"initial_seconds must be positive, got {initial_seconds}"
            raise ValueError(msg)
        if max_seconds < initial_seconds:
            msg = f"max_seconds ({max_seconds}) must be >= initial_seconds ({initial_seconds})"
            raise ValueError(msg)
        self._initial = initial_seconds
        self._max = max_seconds
        self._rng = rng
        self._attempt = 0
        # Number of doublings needed to reach the cap. The exponent is clamped to
        # this rather than left to grow with the attempt count, because
        # `initial * 2**attempt` is evaluated before the cap is applied and
        # `1.0 * 2**1024` raises OverflowError instead of returning inf. At a
        # 60-second ceiling that is roughly 17 hours of continuous failure — long
        # for a laptop, ordinary for a service — and the crash would happen
        # inside the error handler, which is the worst place to have one.
        self._max_shift = 0
        value = initial_seconds
        while value < max_seconds:
            value *= 2
            self._max_shift += 1

    @property
    def attempt(self) -> int:
        """Number of consecutive failures since the last `reset`."""
        return self._attempt

    def next_delay(self) -> float:
        """Record a failure and return how long to wait before retrying."""
        # 2.0 rather than 2: a float base keeps this in float arithmetic instead
        # of building an arbitrary-precision integer that is immediately
        # discarded, and it types as float rather than Any.
        uncapped = self._initial * (2.0 ** min(self._attempt, self._max_shift))
        capped = min(uncapped, self._max)
        self._attempt += 1
        half = capped / 2
        return half + float(self._rng()) * half

    def reset(self) -> None:
        """Forget the failure history.

        Call this only once the connection has actually delivered data, not when
        it has merely been accepted. A server that accepts a connection and
        immediately closes it would otherwise reset the schedule on every
        attempt, turning the backoff into a tight loop.
        """
        self._attempt = 0
