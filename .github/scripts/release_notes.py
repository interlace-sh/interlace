"""Build a GitHub Release title and body from CHANGELOG.md for a version tag."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

_HEADING = re.compile(r"^## (?P<version>\S+)(?:\s+\([^)]+\))?\s*$")
_BOLD = re.compile(r"\*\*(.+?)\*\*", re.DOTALL)
_CODE = re.compile(r"`([^`]*)`")
_WS = re.compile(r"\s+")

_FOOTER = (
    "---\n\n"
    "`pip install 'interlaced=={version}'` · "
    "[full changelog](https://github.com/interlace-sh/interlace/blob/{tag}/CHANGELOG.md) · "
    "[docs](https://interlace.sh/docs)\n"
)


def tag_and_version(raw: str) -> tuple[str, str]:
    tag = raw if raw.startswith("v") else f"v{raw}"
    return tag, tag[1:]


def extract(changelog: str, version: str) -> str:
    lines = changelog.splitlines()
    collecting = False
    body: list[str] = []
    for line in lines:
        heading = _HEADING.match(line)
        if heading is not None:
            if collecting:
                break
            collecting = heading.group("version") == version
            continue
        if collecting:
            body.append(line)
    notes = "\n".join(body).strip()
    if not collecting or not notes:
        raise SystemExit(f"no CHANGELOG.md section for {version}")
    return notes


def title(version: str, notes: str) -> str:
    bold = _BOLD.search(notes)
    if bold is None:
        return version
    headline = _WS.sub(" ", _CODE.sub(r"\1", bold.group(1))).strip().rstrip(".")
    if len(headline) > 80:
        headline = headline[:77].rsplit(" ", 1)[0] + "…"
    return f"{version} — {headline}"


def body(notes: str, version: str, tag: str) -> str:
    return f"{notes}\n\n{_FOOTER.format(version=version, tag=tag)}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", help="2.7.0 or v2.7.0")
    parser.add_argument("--changelog", type=Path, default=Path("CHANGELOG.md"))
    parser.add_argument("--notes-out", type=Path)
    parser.add_argument("--title-out", type=Path)
    args = parser.parse_args()
    tag, version = tag_and_version(args.version)
    notes = extract(args.changelog.read_text(encoding="utf-8"), version)
    notes_text = body(notes, version, tag)
    title_text = title(version, notes)
    if args.notes_out is not None:
        args.notes_out.write_text(notes_text, encoding="utf-8")
    else:
        sys.stdout.write(notes_text)
    if args.title_out is not None:
        args.title_out.write_text(title_text + "\n", encoding="utf-8")
    elif args.notes_out is not None:
        sys.stdout.write(title_text + "\n")


if __name__ == "__main__":
    main()
