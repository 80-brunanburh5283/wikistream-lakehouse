"""Tests that the containers are wired to the code they are supposed to run.

Everything here guards a coupling that spans two files and fails silently rather
than loudly. Three examples, all of which these tests would have caught:

* `dagster.yaml` is only read from `$DAGSTER_HOME`. Mount it one directory away and
  Dagster does not warn — it starts with every default, which here means no run
  queue, no run monitoring, unbounded tick retention and telemetry back on.
* The dbt project is mounted read-only. If the target path or the log path is left
  pointing inside it, dbt fails partway into the first invocation rather than at
  start-up, so the container passes its healthcheck and the first mart build dies.
  That is not hypothetical: the variable was originally named `WS_DBT_TARGET_PATH`,
  which put it outside the mount and out of dagster-dbt's sight at the same time, so
  `dbt build` went back to the default and the mart build died with
  `OSError: Read-only file system`.
* `make dagster-marts` names a job by string. The previous version of that target
  executed `asset_checks_job`, which has never existed in this project.

None of this needs Docker: the assertions are about what the files say.
"""

from __future__ import annotations

import re

import pytest
import yaml
from dagster._config import process_config, resolve_to_config_type
from dagster._core.instance.config import dagster_instance_config_schema

from wikistream_dagster.dbt_project import PROJECT_DIR
from wikistream_dagster.jobs import JOBS

pytestmark = pytest.mark.unit

REPO_ROOT = PROJECT_DIR.parent
DAGSTER_SERVICES = ("dagster-webserver", "dagster-daemon")


@pytest.fixture(scope="session")
def compose() -> dict:
    return yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())


@pytest.fixture(scope="session")
def dockerfile() -> str:
    return (REPO_ROOT / "docker" / "dagster.Dockerfile").read_text()


@pytest.fixture(scope="session")
def makefile() -> str:
    return (REPO_ROOT / "Makefile").read_text()


def _env(dockerfile: str, name: str) -> str:
    """The value of one variable in the Dockerfile's ENV block."""
    match = re.search(rf"^\s*{name}=(\S+)", dockerfile, re.MULTILINE)
    assert match, f"{name} is not set in docker/dagster.Dockerfile"
    return match.group(1)


def _mount(service: dict, container_path: str) -> str:
    """The one mount in this service whose target is `container_path`."""
    matches = [m for m in service["volumes"] if m.split(":")[1] == container_path]
    assert len(matches) == 1, f"expected exactly one mount at {container_path}, got {matches}"
    return matches[0]


def test_the_instance_config_is_valid_for_the_installed_dagster(dockerfile) -> None:
    """dagster.yaml checked against the real schema, not just parsed as YAML.

    An unrecognised key is a hard error at instance load, which in a container means
    a crash loop, and in a Dagster upgrade means a crash loop that starts on a day
    nothing was edited. Cheaper to find here.
    """
    config = yaml.safe_load((REPO_ROOT / "docker" / "dagster" / "dagster.yaml").read_text())
    schema = resolve_to_config_type(dict(dagster_instance_config_schema()))

    result = process_config(schema, config)

    assert result.success, [error.message for error in result.errors]


def test_both_dagster_processes_read_the_same_instance(compose, dockerfile) -> None:
    """One database and one config file between them, or they are not one deployment.

    Separate storage is the failure that looks like success: both containers start,
    both are healthy, the daemon fires its schedules into its own event log and the
    UI shows a deployment where nothing has ever run.
    """
    dagster_home = _env(dockerfile, "DAGSTER_HOME")

    for name in DAGSTER_SERVICES:
        service = compose["services"][name]
        assert _mount(service, dagster_home).startswith("dagster-home:"), name
        # Dagster reads its configuration from exactly this path and reports nothing
        # when it is absent.
        assert _mount(service, f"{dagster_home}/dagster.yaml").endswith(":ro"), name


def test_the_dbt_project_is_read_only_and_dbt_writes_elsewhere(compose, dockerfile) -> None:
    """The mount mode and the two scratch paths are one decision, asserted together."""
    project_dir = _env(dockerfile, "WS_DBT_PROJECT_DIR")

    for name in DAGSTER_SERVICES:
        mount = _mount(compose["services"][name], project_dir)
        assert mount == f"./dbt:{project_dir}:ro", name

    for variable in ("DBT_TARGET_PATH", "DBT_LOG_PATH"):
        path = _env(dockerfile, variable)
        assert not path.startswith(project_dir), (
            f"{variable} is {path}, which is inside the read-only mount"
        )


def test_the_target_path_uses_the_variable_dagster_dbt_reads(dockerfile) -> None:
    """Being outside the read-only mount is not enough; the name has to be dbt's.

    dagster-dbt is never handed the `DbtProject`'s `target_path`. It reads
    `DBT_TARGET_PATH` from the environment and creates its per-invocation directory
    underneath whatever that says, defaulting to a relative `target`. So a correct
    path under a `WS_`-prefixed name is invisible to it: `dbt parse` at start-up
    writes to /tmp, the container goes healthy, and `dbt build` writes to the
    read-only mount and fails.
    """
    source = (REPO_ROOT / "src" / "wikistream_dagster" / "dbt_project.py").read_text()
    entrypoint = (REPO_ROOT / "docker" / "dagster" / "entrypoint.sh").read_text()

    assert 'os.environ.get("DBT_TARGET_PATH"' in source
    for name, text in (
        ("dbt_project.py", source),
        ("Dockerfile", dockerfile),
        ("entrypoint", entrypoint),
    ):
        assert "WS_DBT_TARGET_PATH" not in text, name


def test_the_dbt_service_still_mounts_the_project_writable(compose) -> None:
    """The counterpart to the test above, so `ro` does not spread by copy-paste.

    `make dbt-docs` and a failed-test artefact are only useful if they land in the
    working tree, which is why that container's mount is deliberately different.
    """
    mount = _mount(compose["services"]["dbt"], "/opt/dbt")

    assert not mount.endswith(":ro")


def test_the_entrypoint_parses_the_project_before_starting_dagster(dockerfile) -> None:
    """Because `@dbt_assets` reads the manifest at import time.

    Without the manifest the code location fails to load, and the symptom — an empty
    deployment with an error in the corner of the UI — reads like a broken Dagster
    rather than a missing build step.
    """
    entrypoint = (REPO_ROOT / "docker" / "dagster" / "entrypoint.sh").read_text()

    assert 'ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]' in dockerfile
    assert entrypoint.index("dbt parse") < entrypoint.index('exec "$@"')


def test_both_dagster_services_start_with_make_up(compose) -> None:
    """`make up` is `--profile full`; a Dagster in another profile would never start."""
    for name in DAGSTER_SERVICES:
        assert compose["services"][name]["profiles"] == ["full"], name


def test_the_makefile_only_executes_jobs_that_exist(makefile) -> None:
    """The drift this catches was real: the target used to name a job nobody defined.

    `dagster job execute -j <name>` fails with a non-obvious "no jobs found" message
    rather than naming the job it could not find, so the fastest place to notice is
    here.
    """
    defined = {job.name for job in JOBS}
    invoked = set(re.findall(r"^\t\$\(DAGSTER_EXEC\) (\S+)$", makefile, re.MULTILINE))

    assert invoked, "no Makefile target runs a Dagster job any more"
    assert invoked <= defined, f"Makefile runs undefined jobs: {sorted(invoked - defined)}"
    assert invoked == defined, f"no Makefile target runs: {sorted(defined - invoked)}"
