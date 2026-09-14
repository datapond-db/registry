#!/usr/bin/env python3
"""Regenerate the "Available databases" table in README.md from registry.json.

Usage: python scripts/render_readme_table.py [--check]

--check exits non-zero if README.md is out of date instead of rewriting it.
The table is delimited by the header row and the first blank line after it.
"""
import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def fmt_rows(n):
    return f"{n / 1e9:.2f}B" if n >= 1e9 else f"{n / 1e6:.1f}M" if n >= 1e6 else f"{n / 1e3:.1f}K"


def render(reg):
    lines = ["| Database | Rows | Tables | Size | Coverage | Last rebuilt | Source |",
             "|----------|------|--------|------|----------|--------------|--------|"]
    for d in reg["databases"]:
        lines.append(
            f"| [{d['id']}]({d['github']}) | {fmt_rows(d['rows'])} | {d['tables']} | {d['size_gb']} GB "
            f"| {d.get('data_date_range', '')} | {d.get('last_rebuilt', '')} | {d['source']} |")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    reg = json.loads((ROOT / "registry.json").read_text())
    readme = ROOT / "README.md"
    raw = readme.read_bytes()
    crlf = b"\r\n" in raw
    text = raw.decode().replace("\r\n", "\n")
    pattern = re.compile(r"\| Database \| Rows \|.*?(?=\n\n)", re.S)
    m = pattern.search(text)
    if not m:
        print("README.md: could not find the databases table", file=sys.stderr)
        sys.exit(2)
    new = render(reg)
    if m.group(0) == new:
        print("README.md table is up to date")
        return
    if a.check:
        print("README.md table is OUT OF DATE (run scripts/render_readme_table.py)")
        sys.exit(1)
    text = text[:m.start()] + new + text[m.end():]
    readme.write_bytes((text.replace("\n", "\r\n") if crlf else text).encode())
    total = sum(d["rows"] for d in reg["databases"])
    print(f"README.md table rewritten: {len(reg['databases'])} databases, {total:,} rows")


if __name__ == "__main__":
    main()
