"""Tests that every gold model reads silver to the same cutoff.

The bug this guards against has already happened once, which is why the guard is
here. `silver.edits` gains a snapshot every 30 seconds; a `dbt build` takes longer
than that and runs models on four threads. So two models that both read `stg_edits`
without a shared upper bound read two different snapshots, and a `relationships`
test between them fails for a reason that is in neither model. On 2026-09-18 that is
exactly what happened: `bewiktionary` first appeared mid-run, after `dim_wikis` was
built and before `mart_top_pages_hourly` was, and the reference from the mart to the
dimension broke.

`macros/run_cutoff.sql` fixes it by bounding every model on `run_started_at`, which
is one value per dbt invocation. That fix is invisible — nothing fails if a *new*
model forgets it, until the day two snapshots differ in a way a test can see, and
then it fails intermittently in CI with no obvious cause. These tests are static
reads of the SQL, so they fail immediately and locally instead.

Read from the files rather than from the manifest: the manifest is generated, and a
stale one would let this pass on evidence that no longer matches the tree.
"""

from __future__ import annotations

import re

import pytest

from wikistream_dagster.dbt_project import PROJECT_DIR

pytestmark = pytest.mark.unit

MARTS_DIR = PROJECT_DIR / "models" / "marts"
TESTS_DIR = PROJECT_DIR / "tests"

# The two staging views are the only doors into silver. A model that reads one of
# them is reading a live stream and needs the bound; a model that reads only other
# marts inherits it.
STAGING_REFS = re.compile(r"ref\(\s*'(stg_edits|stg_quarantine)'\s*\)")

# Ingest-time columns, the ones the cutoff is allowed to compare against. Bounding
# on event time would not close the race: a late event has an old event time and a
# new ingest time, so it can still arrive mid-run into a model built after one that
# already read past it.
INGEST_COLUMNS = ("ingested_at", "failed_at")


def strip_commentary(sql: str) -> str:
    """SQL with every comment and Jinja comment removed.

    Without this the assertions below would pass on a file that merely *mentions*
    `run_cutoff` in its header prose, which is the failure mode a documentation-heavy
    codebase invites.
    """
    without_jinja = re.sub(r"\{#.*?#\}", " ", sql, flags=re.DOTALL)
    without_blocks = re.sub(r"/\*.*?\*/", " ", without_jinja, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", without_blocks)


def models_reading_silver() -> list[tuple[str, str]]:
    found = []
    for path in sorted(MARTS_DIR.glob("*.sql")):
        body = strip_commentary(path.read_text())
        if STAGING_REFS.search(body):
            found.append((path.name, body))
    return found


def test_every_mart_reading_silver_is_bounded() -> None:
    models = models_reading_silver()
    # Five: the dimension, three hourly/minutely marts and the health mart. Asserted
    # so that a mart deleted by accident does not make this suite vacuously green.
    assert len(models) == 5, [name for name, _ in models]
    for name, body in models:
        assert "run_cutoff()" in body, f"{name} reads silver without run_cutoff()"


def test_the_cutoff_compares_against_an_ingest_time_column() -> None:
    predicates = {}
    for name, body in models_reading_silver():
        # Only the comparisons. `dim_wikis` also *projects* the cutoff as
        # `built_through`, which is not a predicate and has no column to check.
        matches = list(re.finditer(r"<\s*\{\{\s*run_cutoff\(\)", body))
        predicates[name] = len(matches)
        for match in matches:
            # 120 characters back covers the predicate and its table alias without
            # reaching the previous condition.
            window = body[max(0, match.start() - 120) : match.start()]
            assert any(column in window for column in INGEST_COLUMNS), (
                f"{name} bounds run_cutoff() on something other than "
                f"{' or '.join(INGEST_COLUMNS)}: ...{window[-60:]!r}"
            )
    unbounded = [name for name, count in predicates.items() if count == 0]
    assert not unbounded, f"reads silver but never compares to run_cutoff(): {unbounded}"
    # Six predicates over five models: the health mart bounds two staging views.
    assert sum(predicates.values()) == 6, predicates


def test_run_cutoff_is_applied_to_both_quarantine_and_edits_in_the_health_mart() -> None:
    """The one model that reads both staging views has to bound both of them.

    `mart_pipeline_health` joins two tables on two different clocks, so it is the
    place where bounding one and forgetting the other would look correct.
    """
    body = strip_commentary((MARTS_DIR / "mart_pipeline_health.sql").read_text())
    assert body.count("run_cutoff()") == 2
    assert "ingested_at < " in body
    assert "failed_at < " in body


def test_dim_wikis_publishes_the_cutoff_it_was_built_to() -> None:
    body = strip_commentary((MARTS_DIR / "dim_wikis.sql").read_text())
    assert re.search(r"run_cutoff\(\)\s*\}\}\s+as\s+built_through", body)


def test_the_dimension_test_bounds_itself_on_built_through_not_on_this_run() -> None:
    """`dbt test` alone is a separate invocation with a later cutoff.

    So the singular test that recomputes `dim_wikis` from `stg_edits` has to read its
    bound off the table under test. Using `run_cutoff()` there would reintroduce the
    same race one level up, and only on the runs where `dbt test` is called on its
    own — the hardest version of this bug to reproduce.
    """
    body = strip_commentary(
        (TESTS_DIR / "assert_dim_wikis_primary_domain_was_observed.sql").read_text()
    )
    assert "built_through" in body
    assert "run_cutoff()" not in body
