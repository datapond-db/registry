#!/usr/bin/env python3
"""Add _columns table and generate DICTIONARY.md for any DuckDB database.

Creates a standardized data dictionary inside the database and exports it
as a readable Markdown file.  Idempotent — safe to re-run.

Usage:
    python scripts/add_metadata.py path/to/database.duckdb
    python scripts/add_metadata.py db.duckdb --source-url https://example.gov --license "Public domain"
"""

import argparse
import sys
from pathlib import Path

import duckdb


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def has_table(con, table_name):
    """Return True if *table_name* exists in the main schema."""
    return (
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name = ?",
            [table_name],
        ).fetchone()[0]
        > 0
    )


def get_metadata_columns(con):
    """Return the set of column names in _metadata (empty set if table missing)."""
    if not has_table(con, "_metadata"):
        return set()
    return {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'main' AND table_name = '_metadata'"
        ).fetchall()
    }


def get_user_tables(con):
    """Return sorted list of user table names (excludes _metadata, _columns)."""
    return [
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' "
            "  AND table_name NOT IN ('_metadata', '_columns') "
            "ORDER BY table_name"
        ).fetchall()
    ]


# ---------------------------------------------------------------------------
# Step 1 — ensure _metadata exists and is enriched
# ---------------------------------------------------------------------------


def ensure_metadata(con, source_url=None, license_str=None):
    """Create _metadata if missing; add source_url / license columns if requested."""
    user_tables = get_user_tables(con)

    if not has_table(con, "_metadata"):
        print("  Creating _metadata table from scratch...")
        con.execute(
            "CREATE TABLE _metadata ("
            "  table_name VARCHAR, description VARCHAR, row_count BIGINT, "
            "  column_count INTEGER, source_url VARCHAR, license VARCHAR)"
        )
        for tname in user_tables:
            rc = con.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
            cc = con.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = 'main' AND table_name = ?",
                [tname],
            ).fetchone()[0]
            con.execute(
                "INSERT INTO _metadata (table_name, row_count, column_count) "
                "VALUES (?, ?, ?)",
                [tname, rc, cc],
            )
        print(f"  Created _metadata with {len(user_tables)} entries")

    # Enrich with optional fields
    meta_cols = get_metadata_columns(con)

    if source_url:
        if "source_url" not in meta_cols:
            con.execute("ALTER TABLE _metadata ADD COLUMN source_url VARCHAR")
        con.execute("UPDATE _metadata SET source_url = ?", [source_url])

    if license_str:
        if "license" not in meta_cols:
            con.execute("ALTER TABLE _metadata ADD COLUMN license VARCHAR")
        con.execute("UPDATE _metadata SET license = ?", [license_str])


# ---------------------------------------------------------------------------
# Step 2 — build _columns
# ---------------------------------------------------------------------------


def detect_join_hints(con):
    """Auto-detect likely join columns and return {col_name: hint_str}."""
    hints = {}

    # Rule 1: columns appearing in 3+ tables
    freq = con.execute(
        "SELECT column_name, COUNT(DISTINCT table_name) AS n "
        "FROM information_schema.columns "
        "WHERE table_schema = 'main' "
        "  AND table_name NOT IN ('_metadata', '_columns') "
        "GROUP BY column_name "
        "HAVING COUNT(DISTINCT table_name) >= 3 "
        "ORDER BY n DESC"
    ).fetchall()
    for col, n in freq:
        hints[col] = f"Appears in {n} tables, common join key"

    # Rule 2: *_id or 'id' columns not already flagged
    id_cols = con.execute(
        "SELECT DISTINCT column_name "
        "FROM information_schema.columns "
        "WHERE table_schema = 'main' "
        "  AND table_name NOT IN ('_metadata', '_columns') "
        "  AND (column_name LIKE '%\\_id' ESCAPE '\\' OR column_name = 'id')"
    ).fetchall()
    for (col,) in id_cols:
        if col not in hints:
            hints[col] = "Likely primary or foreign key"

    return hints


def build_columns_table(con):
    """Build the _columns data dictionary table."""
    con.execute("DROP TABLE IF EXISTS _columns")

    # Check if _metadata has source_file column
    meta_cols = get_metadata_columns(con)
    if "source_file" in meta_cols:
        con.execute(
            "CREATE TABLE _columns AS "
            "SELECT c.table_name, c.column_name, c.data_type, m.source_file "
            "FROM information_schema.columns c "
            "LEFT JOIN _metadata m ON m.table_name = c.table_name "
            "WHERE c.table_schema = 'main' "
            "  AND c.table_name NOT IN ('_metadata', '_columns')"
        )
    else:
        con.execute(
            "CREATE TABLE _columns AS "
            "SELECT c.table_name, c.column_name, c.data_type, "
            "  NULL::VARCHAR AS source_file "
            "FROM information_schema.columns c "
            "WHERE c.table_schema = 'main' "
            "  AND c.table_name NOT IN ('_metadata', '_columns')"
        )

    con.execute("ALTER TABLE _columns ADD COLUMN example_value VARCHAR")
    con.execute("ALTER TABLE _columns ADD COLUMN join_hint VARCHAR")
    con.execute("ALTER TABLE _columns ADD COLUMN null_pct DOUBLE")

    # Apply auto-detected join hints
    hints = detect_join_hints(con)
    for col, hint in hints.items():
        con.execute(
            "UPDATE _columns SET join_hint = ? WHERE column_name = ?",
            [hint, col],
        )
    print(f"  Auto-detected {len(hints)} join hint(s)")

    # Populate example_value and null_pct
    rows = con.execute(
        "SELECT table_name, column_name FROM _columns"
    ).fetchall()
    total = len(rows)

    for i, (table_name, column_name) in enumerate(rows):
        if (i + 1) % 200 == 0 or i + 1 == total:
            print(f"  Enriching columns: {i + 1}/{total}", end="\r", flush=True)

        # example_value
        try:
            result = con.execute(
                f'SELECT CAST("{column_name}" AS VARCHAR) '
                f'FROM "{table_name}" '
                f'WHERE "{column_name}" IS NOT NULL LIMIT 1'
            ).fetchone()
            if result and result[0] is not None:
                val = result[0].replace("\x00", "")
                if len(val) > 80:
                    val = val[:77] + "..."
                con.execute(
                    "UPDATE _columns SET example_value = ? "
                    "WHERE table_name = ? AND column_name = ?",
                    [val, table_name, column_name],
                )
        except Exception:
            pass

        # null_pct
        try:
            result = con.execute(
                f'SELECT ROUND(100.0 * COUNT(*) FILTER '
                f'(WHERE "{column_name}" IS NULL) / COUNT(*), 1) '
                f'FROM "{table_name}"'
            ).fetchone()
            if result and result[0] is not None:
                con.execute(
                    "UPDATE _columns SET null_pct = ? "
                    "WHERE table_name = ? AND column_name = ?",
                    [result[0], table_name, column_name],
                )
        except Exception:
            pass

    print()  # newline after \r progress

    col_count = con.execute("SELECT COUNT(*) FROM _columns").fetchone()[0]
    hint_count = con.execute(
        "SELECT COUNT(*) FROM _columns WHERE join_hint IS NOT NULL"
    ).fetchone()[0]
    print(f"  {col_count} columns cataloged, {hint_count} with join hints")


# ---------------------------------------------------------------------------
# Step 3 — export DICTIONARY.md
# ---------------------------------------------------------------------------


def export_dictionary(con, output_path):
    """Export _columns and _metadata as a readable DICTIONARY.md file."""
    meta_cols = get_metadata_columns(con)

    lines = ["# Data Dictionary", ""]

    # Source line
    if "source_url" in meta_cols:
        url = con.execute(
            "SELECT source_url FROM _metadata WHERE source_url IS NOT NULL LIMIT 1"
        ).fetchone()
        if url:
            lines.append(f"Source: {url[0]}")
            lines.append("")

    tables = con.execute(
        "SELECT DISTINCT table_name FROM _columns ORDER BY table_name"
    ).fetchall()

    for (table_name,) in tables:
        lines.append(f"## {table_name}")
        lines.append("")

        # Fetch available metadata
        if has_table(con, "_metadata"):
            # Build dynamic SELECT based on available columns
            select_parts = []
            if "row_count" in meta_cols:
                select_parts.append("row_count")
            if "description" in meta_cols:
                select_parts.append("description")
            if "source_file" in meta_cols:
                select_parts.append("source_file")

            if select_parts:
                meta = con.execute(
                    f"SELECT {', '.join(select_parts)} FROM _metadata "
                    f"WHERE table_name = ?",
                    [table_name],
                ).fetchone()
                if meta:
                    idx = 0
                    if "description" in meta_cols:
                        desc_idx = select_parts.index("description")
                        desc = meta[desc_idx]
                        if desc:
                            lines.append(desc)
                            lines.append("")
                    if "source_file" in meta_cols:
                        sf_idx = select_parts.index("source_file")
                        sf = meta[sf_idx]
                        if sf:
                            lines.append(f"Source file: `{sf}`")
                    if "row_count" in meta_cols:
                        rc_idx = select_parts.index("row_count")
                        rc = meta[rc_idx]
                        if rc:
                            lines.append(f"Rows: {rc:,}")
                    lines.append("")

        lines.append("| Column | Type | Nulls | Example | Join |")
        lines.append("|--------|------|-------|---------|------|")

        cols = con.execute(
            "SELECT column_name, data_type, null_pct, example_value, join_hint "
            "FROM _columns WHERE table_name = ? ORDER BY rowid",
            [table_name],
        ).fetchall()

        for col_name, dtype, null_pct, example, join_hint in cols:
            null_str = f"{null_pct:.1f}%" if null_pct is not None else ""
            example_str = (example or "").replace("|", "\\|")
            join_str = join_hint or ""
            lines.append(
                f"| {col_name} | {dtype} | {null_str} | {example_str} | {join_str} |"
            )

        lines.append("")

    content = "\n".join(lines)
    content = content.replace("\x00", "")

    with open(output_path, "w") as f:
        f.write(content)

    print(f"  Exported {output_path} ({len(tables)} tables)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Add _columns table and generate DICTIONARY.md for a DuckDB database"
    )
    parser.add_argument("db_path", type=Path, help="Path to the .duckdb file")
    parser.add_argument("--source-url", help="Source URL to store in _metadata")
    parser.add_argument("--license", dest="license_str", help="License to store in _metadata")
    parser.add_argument(
        "--output-dir", type=Path, default=None,
        help="Directory for DICTIONARY.md (default: same as .duckdb file)",
    )
    args = parser.parse_args()

    if not args.db_path.exists():
        print(f"Error: {args.db_path} not found")
        sys.exit(1)

    output_dir = args.output_dir or args.db_path.parent
    dict_path = output_dir / "DICTIONARY.md"

    print(f"Opening {args.db_path}")
    con = duckdb.connect(str(args.db_path))

    print("\n[1/3] Ensuring _metadata")
    ensure_metadata(con, args.source_url, args.license_str)

    print("\n[2/3] Building _columns")
    build_columns_table(con)

    print("\n[3/3] Exporting DICTIONARY.md")
    export_dictionary(con, dict_path)

    # Summary
    total_rows = con.execute(
        "SELECT SUM(row_count) FROM _metadata"
    ).fetchone()[0]
    n_tables = con.execute(
        "SELECT COUNT(*) FROM _metadata"
    ).fetchone()[0]
    n_cols = con.execute("SELECT COUNT(*) FROM _columns").fetchone()[0]
    print(f"\nDone. {n_tables} tables, {total_rows:,} total rows, {n_cols} columns")

    con.close()


if __name__ == "__main__":
    main()
