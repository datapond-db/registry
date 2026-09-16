#!/usr/bin/env python3
"""Add or refresh the per-table `_metadata` / `_columns` dictionary tables in a DuckDB file
and export DICTIONARY.md. A thin wrapper over the shared implementation in datapond-build
(https://github.com/datapond-db/datapond-build), so contributed databases get exactly the
layout the clients expect. Safe to re-run: existing row counts are refreshed, tables that no
longer exist are dropped from `_metadata`, and `_columns` is rebuilt.

Usage:
    pip install "datapond-build @ git+https://github.com/datapond-db/datapond-build@v0.1.2"
    python scripts/add_metadata.py path/to/database.duckdb [--source-url URL] [--license TEXT]
        [--description table=text ...] [--join-hint column=text ...] [--dictionary DICTIONARY.md]

Join hints are curated by the maintainer (a column name -> what it joins to); nothing is
guessed from column names.
"""
import argparse
import sys
from pathlib import Path

import duckdb

try:
    from datapond_build import build_columns_table, ensure_metadata, export_dictionary, user_tables
except ImportError:
    sys.exit("datapond-build is required: pip install 'datapond-build @ git+https://github.com/datapond-db/datapond-build@v0.1.2'")


def kv(pairs):
    out = {}
    for item in pairs or []:
        k, _, v = item.partition("=")
        out[k] = v
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("db", type=Path)
    ap.add_argument("--source-url")
    ap.add_argument("--license")
    ap.add_argument("--description", action="append", metavar="TABLE=TEXT")
    ap.add_argument("--join-hint", action="append", metavar="COLUMN=TEXT")
    ap.add_argument("--dictionary", type=Path, default=None, help="DICTIONARY.md path (default: next to the database)")
    ap.add_argument("--title", default=None)
    a = ap.parse_args(argv)
    con = duckdb.connect(str(a.db))
    tables = user_tables(con)
    # replace=True rebuilds _metadata from the tables that exist now, so a rerun after
    # adding rows or tables never leaves stale counts or orphan entries behind
    n = ensure_metadata(con, descriptions=kv(a.description), tables=tables, source_url=a.source_url,
                        license=a.license, replace=True)
    m = build_columns_table(con, join_hints=kv(a.join_hint), tables=tables, quiet=True)
    out = a.dictionary or a.db.with_name("DICTIONARY.md")
    export_dictionary(con, out, title=a.title or f"{a.db.stem} Data Dictionary", tables=tables, style="registry")
    con.execute("CHECKPOINT")
    con.close()
    print(f"_metadata: {n} tables; _columns: {m} columns; wrote {out}")


if __name__ == "__main__":
    main()
