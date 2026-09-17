"""The schema-evolution proof against the real catalog, and the debris it must not leave.

`scripts/prove_schema_evolution.py` makes seventeen checks of its own and exits non-zero
if any of them fails, so this module does not repeat them one by one. It asserts the two
things the script cannot assert about itself:

* **It cleaned up after a run that passed.** A demonstration that leaves a scratch table
  and a namespace in the catalog is worse than no demonstration, and the cleanup lives in
  a `finally` block that no check inside the script can see the result of.
* **It will not run against the real silver namespace.** The whole point of the scratch
  namespace is that `silver.edits` never ends up carrying a permanently null `edit_source`
  column. That guard is one `if` and needs a test, not trust.

The claim names are listed here rather than counted, because a script that had been gutted
down to two trivial checks would still print "all checks passed".
"""

from __future__ import annotations

import shutil
import uuid
from typing import TYPE_CHECKING

import pytest
from stack import query, spark_submit

from wikistream.config import Settings

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

SCRIPT = "/opt/wikistream/scripts/prove_schema_evolution.py"

#: One claim per schema change the pipeline could need, by the name the script prints.
#: Iceberg's promise is per-operation, and a run that only exercised ADD COLUMN would
#: prove nothing about the destructive ones.
REQUIRED_CLAIMS = (
    "ADD COLUMN rewrote no data file",
    "rows written before the column read null",
    "a row written after it carries the value",
    "FOR VERSION AS OF sees the pre-change table",
    "FOR TIMESTAMP AS OF resolves to the same snapshot",
    "RENAME COLUMN rewrote no data file",
    "the value survived the rename",
    "namespace_id widened to bigint",
    "narrowing bigint to int is refused",
    "DROP COLUMN rewrote no data file",
    "every row still reads",
)


@pytest.fixture(scope="module")
def proof() -> Iterator[tuple[subprocess.CompletedProcess[str], str]]:
    """Run the script once against a namespace named for this run, and keep its output.

    Module-scoped because the run costs a JVM start and both assertions read the same
    result. The namespace carries a random suffix so two runs — or a run alongside a
    human's `make prove-schema-evolution` — cannot collide on the same scratch table.
    """
    if shutil.which("docker") is None:
        pytest.skip("docker not on PATH")

    namespace = f"schema_evolution_it_{uuid.uuid4().hex[:8]}"
    yield spark_submit(SCRIPT, "--namespace", namespace, env={}), namespace


def test_every_schema_change_is_metadata_only(proof):
    result, _ = proof
    passed = {
        line.split("]", 1)[1].strip()
        for line in result.stdout.splitlines()
        if line.strip().startswith("[pass]")
    }
    for claim in REQUIRED_CLAIMS:
        assert any(line.startswith(claim) for line in passed), f"no passing check named {claim!r}"
    assert "FAIL" not in result.stdout
    assert f"all {len(passed)} checks passed" in result.stdout


def test_the_demonstration_leaves_no_table_behind(proof):
    _, namespace = proof
    namespaces = {row["namespace"] for row in query("SHOW NAMESPACES IN lakehouse", env={})}
    assert namespace not in namespaces, f"{namespace} survived a passing run"


def test_it_refuses_to_run_against_the_real_silver_namespace():
    if shutil.which("docker") is None:
        pytest.skip("docker not on PATH")

    result = spark_submit(
        SCRIPT, "--namespace", Settings().silver_namespace, env={}, check=False, timeout=120
    )
    assert result.returncode != 0
    assert "refusing to run against the real silver namespace" in result.stdout + result.stderr
