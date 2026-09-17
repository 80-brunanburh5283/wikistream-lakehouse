"""Tests for the reconnect delay policy.

The point of testing arithmetic this small is that its failure mode is invisible:
a backoff that silently never grows looks exactly like a healthy client until the
upstream starts refusing connections.
"""

from __future__ import annotations

import pytest

from wikistream.sources.backoff import ExponentialBackoff

pytestmark = pytest.mark.unit


def _fixed(value: float):
    """An rng that always returns `value`, so delays become exact."""
    return lambda: value


def test_delay_doubles_each_attempt():
    backoff = ExponentialBackoff(1.0, 60.0, rng=_fixed(0.0))
    # With rng=0 the delay is the lower bound of the equal-jitter window, which
    # is half the un-jittered delay: 0.5, 1, 2, 4, 8.
    assert [backoff.next_delay() for _ in range(5)] == [0.5, 1.0, 2.0, 4.0, 8.0]


def test_delay_is_capped():
    backoff = ExponentialBackoff(1.0, 10.0, rng=_fixed(1.0))
    delays = [backoff.next_delay() for _ in range(12)]
    assert max(delays) == 10.0
    assert delays[-1] == 10.0


def test_jitter_stays_within_the_equal_jitter_window():
    # Equal jitter means the delay is in [half, full]. The lower bound is the
    # property that matters: full jitter can return ~0 on a late attempt and
    # briefly recreate the tight reconnect loop the backoff exists to stop.
    for value in (0.0, 0.25, 0.5, 0.99):
        backoff = ExponentialBackoff(2.0, 60.0, rng=_fixed(value))
        for attempt in range(6):
            uncapped = min(2.0 * 2**attempt, 60.0)
            delay = backoff.next_delay()
            assert uncapped / 2 <= delay <= uncapped


def test_delay_is_never_zero_even_with_a_zero_rng():
    backoff = ExponentialBackoff(1.0, 60.0, rng=_fixed(0.0))
    assert all(backoff.next_delay() > 0 for _ in range(20))


def test_attempt_counts_consecutive_failures():
    backoff = ExponentialBackoff(1.0, 60.0, rng=_fixed(0.5))
    assert backoff.attempt == 0
    backoff.next_delay()
    backoff.next_delay()
    assert backoff.attempt == 2


def test_reset_returns_to_the_initial_delay():
    backoff = ExponentialBackoff(1.0, 60.0, rng=_fixed(0.0))
    for _ in range(5):
        backoff.next_delay()
    backoff.reset()
    assert backoff.attempt == 0
    assert backoff.next_delay() == 0.5


def test_delay_is_stable_long_after_the_cap_is_reached():
    backoff = ExponentialBackoff(1.0, 60.0, rng=_fixed(0.5))
    for _ in range(200):
        delay = backoff.next_delay()
    assert delay == pytest.approx(45.0)


def test_thousands_of_failures_do_not_raise_overflowerror():
    # Regression test. `initial * 2**attempt` is evaluated before the cap, and
    # `1.0 * 2**1024` raises OverflowError rather than returning inf. Reaching
    # attempt 1024 at a 60s ceiling takes about 17 hours of continuous failure,
    # so the original bug would have surfaced as a crash inside the reconnect
    # handler of a process that had been quietly retrying overnight.
    backoff = ExponentialBackoff(1.0, 60.0, rng=_fixed(0.5))
    for _ in range(2_000):
        delay = backoff.next_delay()
    assert delay == pytest.approx(45.0)


@pytest.mark.parametrize("initial", [0.0, -1.0])
def test_non_positive_initial_is_rejected(initial):
    with pytest.raises(ValueError, match="initial_seconds must be positive"):
        ExponentialBackoff(initial, 60.0)


def test_max_below_initial_is_rejected():
    with pytest.raises(ValueError, match="must be >= initial_seconds"):
        ExponentialBackoff(10.0, 5.0)


def test_equal_initial_and_max_is_allowed():
    backoff = ExponentialBackoff(5.0, 5.0, rng=_fixed(0.0))
    assert backoff.next_delay() == 2.5
