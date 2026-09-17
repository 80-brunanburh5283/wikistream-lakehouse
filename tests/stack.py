"""Driving the running compose stack from a test: `docker compose`, `spark-submit`, SQL.

Every integration test needs the same three things — run a command in a container, run
a Spark script with per-test `WS_*` overrides, and get the rows back as Python. Three
modules had grown their own copies of all three by the time this file was written.

Not in `conftest.py` because these are plain functions and wrapping each one in a
fixture would mean threading three extra arguments through every test signature to buy
nothing. `tests` is on `sys.path` via the `pythonpath` setting in pyproject.toml, so
`from stack import query, spark_submit` works from any test module — the same route
`doubles.py` takes.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Long enough for a cold JVM start plus a micro-batch on a loaded laptop. A bounded
#: streaming run that has not finished in five minutes is broken, not slow.
SUBMIT_TIMEOUT_SECONDS = 300

SPARK_SQL_SCRIPT = "/opt/wikistream/scripts/spark_sql.py"


def compose(*args: str, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    """Run a `docker compose` command from the repository root."""
    # Fixed argv, no shell, `docker` resolved from PATH.
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def spark_submit(
    script: str,
    *script_args: str,
    env: dict[str, str],
    timeout: int = SUBMIT_TIMEOUT_SECONDS,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run a script in the Spark container with `WS_*` overrides for this test run.

    `check=False` is for the tests whose subject *is* the exit code — the duplicate
    gate, which has to be shown failing as well as passing. Everything else wants the
    default, because a green test that silently ignored a non-zero exit is worse than a
    red one.
    """
    env_flags = [flag for key, value in env.items() for flag in ("-e", f"{key}={value}")]
    result = compose(
        "exec",
        "-T",
        *env_flags,
        "spark",
        "/opt/spark/bin/spark-submit",
        script,
        *script_args,
        timeout=timeout,
    )
    if check and result.returncode != 0:
        # The tail, not the whole log: a Spark failure puts the useful lines last
        # and the first two hundred are class loading.
        tail = "\n".join(result.stdout.strip().splitlines()[-40:])
        pytest.fail(f"{script} exited {result.returncode}\n{tail}")
    return result


def query(sql: str, env: dict[str, str]) -> list[dict[str, Any]]:
    """Run one statement in the container and parse the JSON rows it prints.

    Every line that starts with `{` is a row: `spark-submit` merges the Python
    process's stderr into stdout, so filtering by stream is not possible. See the
    docstring of `scripts/spark_sql.py`.
    """
    result = spark_submit(SPARK_SQL_SCRIPT, "--json", sql, env=env)
    return [
        json.loads(line)
        for line in result.stdout.splitlines()
        if line.startswith("{") and line.rstrip().endswith("}")
    ]


def scalar(sql: str, env: dict[str, str]) -> Any:
    """The first column of the first row, for the many queries that return one number."""
    rows = query(sql, env)
    if not rows:
        raise AssertionError(f"expected one row, got none, from: {sql}")
    return next(iter(rows[0].values()))
