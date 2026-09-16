#!/usr/bin/env python3
"""Update one registry.json entry from a freshly built local .duckdb.

Usage: python scripts/update_registry.py --id eoir --db /path/eoir.duckdb [--date 2026-09-15] [--data-date-range "1996-2026"]

Sets rows (sum of _metadata.row_count), tables (count of _metadata rows), size_gb,
last_rebuilt and updated (today unless --date), and data_date_range (from a
per-database max-date query unless given). Preserves key order.
"""
import argparse, json, datetime
from pathlib import Path
import duckdb

REG = Path(__file__).resolve().parent.parent / "registry.json"

# How to derive the "through" year for data_date_range, per database.
RANGE_SQL = {
    "eoir": ("1996", "SELECT EXTRACT(YEAR FROM MAX(OSC_DATE)) FROM proceedings WHERE OSC_DATE <= CURRENT_DATE"),
    "ice": ("2003", "SELECT EXTRACT(YEAR FROM MAX(apprehension_date)) FROM arrests WHERE apprehension_date <= CURRENT_DATE"),
    "fec": ("2004", "SELECT MAX(cycle) FROM individual_contributions"),
    "clinicaltrials": ("2000", "SELECT EXTRACT(YEAR FROM MAX(study_first_submitted_date)) FROM studies"),
    "dol-visas": ("FY2015", "SELECT 'FY' || MAX(fiscal_year) FROM lca"),
    "cms-medicare": ("2013", "SELECT MAX(year) FROM physician_services"),
    "openpayments": ("2013", "SELECT MAX(program_year) FROM general_payments"),
}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True); ap.add_argument("--db", required=True, type=Path)
    ap.add_argument("--date", default=datetime.date.today().isoformat())
    ap.add_argument("--data-date-range")
    a = ap.parse_args()
    reg = json.loads(REG.read_text())
    entry = next(d for d in reg["databases"] if d["id"] == a.id)
    con = duckdb.connect(str(a.db), read_only=True)
    n_tables, rows = con.execute("SELECT COUNT(*), SUM(row_count) FROM _metadata").fetchone()
    rng = a.data_date_range
    if not rng and a.id in RANGE_SQL:
        start, sql = RANGE_SQL[a.id]
        end = con.execute(sql).fetchone()[0]
        rng = f"{start}-{int(end) if isinstance(end, float) else end}"
    con.close()
    old = {k: entry[k] for k in ("rows", "tables", "size_gb", "data_date_range", "last_rebuilt", "updated")}
    entry["rows"] = int(rows); entry["tables"] = int(n_tables)
    size_gb = a.db.stat().st_size / 1024**3
    # one decimal for normal files; small files keep enough precision not to display as 0.0 GB
    entry["size_gb"] = round(size_gb, 1) if size_gb >= 0.1 else round(size_gb, 3)
    if rng: entry["data_date_range"] = rng
    entry["last_rebuilt"] = a.date; entry["updated"] = a.date
    text = json.dumps(reg, indent=2, ensure_ascii=False) + "\n"
    REG.write_bytes(text.replace("\n", "\r\n").encode() if b"\r\n" in REG.read_bytes() else text.encode())
    for k in old:
        print(f"  {k}: {old[k]} -> {entry[k]}")
    print(f"updated registry.json entry for {a.id}")

if __name__ == "__main__":
    main()
