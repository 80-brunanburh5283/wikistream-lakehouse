"""Tests for the Markdown link checker, and the checker run against this repository.

Two jobs in one file. The first half tests the parts that are easy to get wrong —
GitHub's anchor slugs, and the fenced blocks that must not be read as prose. The
second half is the regression guard: `check()` over the real tree must return
nothing, so a reworded heading fails here rather than in a reviewer's browser.

The slug cases are not invented. Every one of them is a heading shape that exists in
`DECISIONS.md` or `README.md`, because the failure mode of a hand-written slugifier
is that it works on `## Simple Heading` and disagrees with GitHub on exactly the
punctuation this repository uses.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_checker():
    """Import `scripts/check_doc_links.py` by path.

    `scripts/` is a directory of entry points rather than a package — every file in
    it is something `make` runs — so there is no module path to import. Loading by
    location keeps it that way instead of adding an `__init__.py` for the tests'
    convenience.
    """
    path = REPO_ROOT / "scripts" / "check_doc_links.py"
    spec = importlib.util.spec_from_file_location("check_doc_links", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


checker = _load_checker()


@pytest.mark.parametrize(
    ("heading", "slug"),
    [
        # The em dash is dropped and the spaces either side of it are not, which is
        # where the double hyphen in every ADR anchor comes from.
        (
            "ADR-0041 — Iceberg over Delta Lake and Hudi",
            "adr-0041--iceberg-over-delta-lake-and-hudi",
        ),
        # Backticks vanish; the underscore inside the identifier does not.
        (
            "ADR-0006 — Keep the polymorphic `log_params` as raw JSON text",
            "adr-0006--keep-the-polymorphic-log_params-as-raw-json-text",
        ),
        # A trailing `*` and a comma go, leaving a bare underscore against a hyphen.
        (
            "ADR-0018 — Keep ANSI mode on and parse with `try_*`, rather than disabling it",
            "adr-0018--keep-ansi-mode-on-and-parse-with-try_-rather-than-disabling-it",
        ),
        # An apostrophe and a semicolon are removed rather than replaced, so neither
        # leaves a hyphen behind: `Spark's` slugs to `sparks`.
        (
            "ADR-0022 — The parse verdict is Spark's alone; only the rules have a Python twin",
            "adr-0022--the-parse-verdict-is-sparks-alone-only-the-rules-have-a-python-twin",
        ),
        (
            "Where the disk actually went: three counts, three answers",
            "where-the-disk-actually-went-three-counts-three-answers",
        ),
        # A heading that is itself a link slugs its label, not its URL.
        ("See [the runbook](docs/runbook.md)", "see-the-runbook"),
    ],
)
def test_slugify_matches_github(heading, slug):
    assert checker.slugify(heading) == slug


def test_repeated_headings_get_github_s_numeric_suffix():
    # `### Context` appears once per ADR in DECISIONS.md. Without the counter a
    # checker accepts `#context-99` and catches nothing.
    found = checker.anchors("### Context\ntext\n### Context\ntext\n### Context\n")
    assert found == {"context", "context-1", "context-2"}


def test_fenced_blocks_are_not_prose():
    markdown = (
        "# Real heading\n"
        "```bash\n"
        "# not a heading, a shell comment\n"
        "curl [not](a-link.md)\n"
        "```\n"
        "~~~\n"
        "# also not a heading\n"
        "~~~\n"
    )
    assert checker.anchors(markdown) == {"real-heading"}
    assert list(checker.links(Path("x.md"), markdown)) == []


def test_line_numbers_survive_fence_stripping():
    markdown = "```\nquoted\n```\n[link](target.md)\n"
    (found,) = checker.links(Path("x.md"), markdown)
    assert (found.line, found.target) == (4, "target.md")


@pytest.mark.parametrize(
    "target",
    ["https://example.com/a.md", "http://example.com", "mailto:someone@example.com"],
)
def test_absolute_targets_are_somebody_else_s_problem(target):
    assert list(checker.links(Path("x.md"), f"[a]({target})")) == []


def test_a_broken_file_and_a_broken_anchor_are_both_reported(tmp_path):
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "target.md").write_text("## The Section\n")
    (tmp_path / "index.md").write_text(
        "[fine](docs/target.md)\n"
        "[fine too](docs/target.md#the-section)\n"
        "[gone](docs/missing.md)\n"
        "[reworded](docs/target.md#the-old-name)\n"
    )
    failures = checker.check(tmp_path, files=[Path("index.md")])
    assert len(failures) == 2
    assert "index.md:3: no such file: docs/missing.md" in failures[0]
    assert "index.md:4: no such heading: docs/target.md#the-old-name" in failures[1]


def test_same_file_anchors_are_checked_against_the_file_itself(tmp_path):
    (tmp_path / "page.md").write_text(
        "## Known limitations\n[up](#known-limitations)\n[no](#nope)\n"
    )
    failures = checker.check(tmp_path, files=[Path("page.md")])
    assert len(failures) == 1
    assert "no such heading: #nope" in failures[0]


def test_a_link_to_a_directory_is_accepted(tmp_path):
    (tmp_path / "infra").mkdir()
    (tmp_path / "page.md").write_text("[the module](infra/)\n")
    assert checker.check(tmp_path, files=[Path("page.md")]) == []


def test_pull_request_template_links_resolve_from_the_repository_root(tmp_path):
    # `.github/PULL_REQUEST_TEMPLATE.md` writes `../blob/main/docs/runbook.md`, which
    # GitHub resolves from the pull-request page. Resolved from the template's own
    # directory it would point at `.github/docs/`, so the prefix has to be understood
    # rather than skipped.
    (tmp_path / ".github").mkdir()
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "runbook.md").write_text("# Runbook\n")
    (tmp_path / ".github" / "PULL_REQUEST_TEMPLATE.md").write_text(
        "[runbook](../blob/main/docs/runbook.md)\n[gone](../blob/main/docs/nope.md)\n"
    )
    failures = checker.check(tmp_path, files=[Path(".github/PULL_REQUEST_TEMPLATE.md")])
    assert len(failures) == 1
    assert "no such file: ../blob/main/docs/nope.md" in failures[0]


def test_every_relative_link_in_this_repository_resolves():
    failures = checker.check(REPO_ROOT)
    assert failures == [], "\n".join(failures)


def test_the_checker_reads_the_documents_it_is_meant_to_read():
    # A checker that silently found no files would pass the test above forever. The
    # README, the decision log and the pages under docs/ are the ones with links in
    # them, so assert they are in scope.
    tracked = set(checker.tracked_markdown(REPO_ROOT))
    assert Path("README.md") in tracked
    assert Path("DECISIONS.md") in tracked
    assert len([name for name in tracked if name.parts[0] == "docs"]) >= 7
