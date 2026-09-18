#!/usr/bin/env python
"""Check that every relative Markdown link in this repository resolves.

    make check-links

This repository is mostly prose: a README that links 19 architecture decision
records by heading anchor, seven pages under `docs/` that cross-reference each
other, and a pull-request template that links back into the tree. A link to a
heading that has been reworded does not fail any test, does not fail `make lint`,
and looks exactly like a working link until a reviewer clicks it and gets the top
of the page instead of the section they were promised. That is a bad first
impression for a defect nobody can see while writing.

So this walks the tracked Markdown and resolves three things on disk:

* relative file links (`docs/runbook.md`, `../DECISIONS.md`) — the file must exist,
* heading anchors (`DECISIONS.md#adr-0020--merge-into-inside-foreachbatch`) — the
  target file must contain a heading whose GitHub slug is exactly that fragment,
* same-file anchors (`#and-on-gcp`).

Only tracked files are checked, which is not a performance choice: the local build
notes are gitignored, so asking git for the file list is what keeps them out of
scope without naming them here.

Exits 1 and prints every failure with `file:line`, so it is usable from CI and from
pre-commit rather than being a report someone reads.
"""

from __future__ import annotations

import re
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple

# GitHub renders relative links in a pull-request or issue body against the page
# URL, not against the repository root, so `.github/PULL_REQUEST_TEMPLATE.md`
# writes `../blob/main/docs/runbook.md` — which resolves correctly from
# `https://…/pull/1` and resolves to nothing at all on disk. Rather than skip
# those links, strip the prefix and check the path they point at.
GITHUB_BLOB_PREFIX = re.compile(r"^\.\./blob/[^/]+/")

# Fences open and close with at least three backticks or tildes. The README quotes
# console output and a mermaid diagram; both contain text that looks like a link or
# a heading, and neither is one.
FENCE = re.compile(r"^\s*(`{3,}|~{3,})")

# `](target)` covers links and images alike. Titles (`](path "title")`) are not used
# in this repository, and a target containing whitespace would be broken anyway.
LINK = re.compile(r"\]\((?P<target>[^)\s]+)\)")

HEADING = re.compile(r"^#{1,6}\s+(?P<text>.*?)\s*#*\s*$")

# The scheme half of a URL, plus the mailto and protocol-relative forms. Anything
# matching this is somebody else's server and not this checker's business.
ABSOLUTE = re.compile(r"^([a-z][a-z0-9+.-]*:|//)", re.IGNORECASE)

# github-slugger keeps letters, digits, `_` and `-`, drops everything else, and turns
# each remaining whitespace character into a hyphen. The characters that matter here
# are the em dash in every ADR heading and the backticks around every identifier:
# both vanish, and the space either side of the em dash leaves the double hyphen that
# makes `#adr-0020--merge-into-…` look like a typo when it is not.
SLUG_DROP = re.compile(r"[^\w\s-]")


class Link(NamedTuple):
    """One relative link, with enough context to name it in an error message."""

    source: Path
    line: int
    target: str


def slugify(heading: str) -> str:
    """The anchor GitHub generates for a heading, before de-duplication.

    Reimplemented rather than depended on: github-slugger is a JavaScript package,
    and adding a Node toolchain to a Python repository to check its own prose is a
    worse trade than forty lines with a test beside them.
    """
    text = re.sub(r"\[(?P<label>[^\]]*)\]\([^)]*\)", r"\g<label>", heading)
    return re.sub(r"\s", "-", SLUG_DROP.sub("", text.strip().lower()))


def anchors(markdown: str) -> set[str]:
    """Every anchor a reader can link to in one file.

    Repeated headings get `-1`, `-2` and so on, exactly as GitHub does — `### Context`
    appears once per ADR in `DECISIONS.md`, so a checker without this would accept
    `#context-45` and reject nothing.
    """
    seen: dict[str, int] = {}
    found: set[str] = set()
    for line in strip_fences(markdown).splitlines():
        match = HEADING.match(line)
        if match is None:
            continue
        base = slugify(match.group("text"))
        if not base:
            continue
        count = seen.get(base, 0)
        seen[base] = count + 1
        found.add(base if count == 0 else f"{base}-{count}")
    return found


def strip_fences(markdown: str) -> str:
    """Blank the inside of every fenced block, keeping the line count intact.

    Line numbers are the whole point of the error messages, so the fenced content is
    replaced rather than removed.
    """
    out: list[str] = []
    closing: str | None = None
    for line in markdown.splitlines():
        fence = FENCE.match(line)
        if closing is None:
            if fence is not None:
                closing = fence.group(1)[0] * 3
                out.append("")
                continue
            out.append(line)
        else:
            if fence is not None and fence.group(1).startswith(closing):
                closing = None
            out.append("")
    return "\n".join(out)


def links(path: Path, markdown: str) -> Iterator[Link]:
    """Every relative link in one file, in source order."""
    for number, line in enumerate(strip_fences(markdown).splitlines(), start=1):
        for match in LINK.finditer(line):
            target = match.group("target")
            if ABSOLUTE.match(target):
                continue
            yield Link(source=path, line=number, target=target)


def tracked_markdown(root: Path) -> list[Path]:
    """The Markdown files git knows about, as paths relative to the repository root."""
    listed = subprocess.run(
        ["git", "-C", str(root), "ls-files", "*.md"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [Path(name) for name in listed.stdout.split()]


def check(root: Path, files: list[Path] | None = None) -> list[str]:
    """Every broken relative link in the tree, as printable messages.

    `files` is relative to `root` and defaults to everything git tracks. It is a
    parameter so that the tests can point this at a fixture tree without making a
    git repository out of a temporary directory.
    """
    cache: dict[Path, set[str]] = {}
    failures: list[str] = []

    for relative in tracked_markdown(root) if files is None else files:
        source = root / relative
        for link in links(relative, source.read_text(encoding="utf-8")):
            path, _, fragment = link.target.partition("#")
            stripped = GITHUB_BLOB_PREFIX.sub("", path)
            if not path:
                target = source.resolve()
            elif stripped != path:
                # A pull-request-body link. Its path is repository-root-relative once
                # the `../blob/<branch>/` prefix is gone, not relative to the template.
                target = (root / stripped).resolve()
            else:
                target = (source.parent / path).resolve()

            where = f"{link.source}:{link.line}"
            if not target.exists():
                failures.append(f"{where}: no such file: {link.target}")
                continue
            # A link to a directory is legitimate — GitHub renders the tree — and a
            # directory has no headings to anchor into.
            if not fragment or target.suffix != ".md" or not target.is_file():
                continue
            if target not in cache:
                cache[target] = anchors(target.read_text(encoding="utf-8"))
            if fragment not in cache[target]:
                failures.append(f"{where}: no such heading: {link.target}")

    return failures


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    failures = check(root)
    if failures:
        print(f"{len(failures)} broken link(s):", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1
    print(f"all relative links resolve in {len(tracked_markdown(root))} tracked files")
    return 0


if __name__ == "__main__":
    sys.exit(main())
