#!/usr/bin/env python3
"""
Batch ETL pipeline (demo): extract -> validate -> load, with a
data-quality layer that reverts and quarantines bad data.

Reads every file in data/incoming/ (CSV or JSON), validates all rows,
and loads good files into a SQLite warehouse. If ANY row in a file is
invalid, the whole file is rejected: nothing from it is loaded (the
"revert"), the file is moved to data/quarantine/, and the run continues
with the remaining files. Fix the file, drop it back in incoming/, and
rerun — loads are keyed by (source_file, row_id) with INSERT OR REPLACE,
so a reload never duplicates data.

Usage:
    python pipeline.py
Outputs:
    warehouse.db          - SQLite database with the `sales` table
    data/quarantine/      - rejected files land here
    Console summary with row counts and elapsed time vs the SLA budget.
"""

import csv
import json
import shutil
import sqlite3
import time
from pathlib import Path

BASE = Path(__file__).parent
INCOMING = BASE / "data" / "incoming"
QUARANTINE = BASE / "data" / "quarantine"
DB_PATH = BASE / "warehouse.db"
SLA_SECONDS = 3 * 60 * 60  # the pattern's point: a hard window, measured every run

REQUIRED = {"row_id": int, "date": str, "store_id": str, "product": str,
            "quantity": int, "amount": float}


def read_rows(path: Path):
    if path.suffix == ".csv":
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    if path.suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    raise ValueError(f"unsupported file type: {path.suffix}")


def validate(rows):
    """Return a list of problems; empty means the file is clean."""
    problems = []
    for i, row in enumerate(rows, 1):
        for field, kind in REQUIRED.items():
            if field not in row or row[field] in ("", None):
                problems.append(f"row {i}: missing {field}")
                continue
            try:
                kind(row[field])
            except (TypeError, ValueError):
                problems.append(f"row {i}: {field}={row[field]!r} is not a valid {kind.__name__}")
        if not problems or True:
            try:
                if int(row.get("quantity", 0)) < 0:
                    problems.append(f"row {i}: negative quantity")
                if float(row.get("amount", 0)) < 0:
                    problems.append(f"row {i}: negative amount")
            except (TypeError, ValueError):
                pass  # already reported above
    return problems


def load(conn, source_file: str, rows):
    conn.executemany(
        "INSERT OR REPLACE INTO sales VALUES (?,?,?,?,?,?,?)",
        [
            (source_file, int(r["row_id"]), r["date"], r["store_id"],
             r["product"], int(r["quantity"]), float(r["amount"]))
            for r in rows
        ],
    )


def main():
    started = time.time()
    QUARANTINE.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS sales (
               source_file TEXT, row_id INTEGER, date TEXT, store_id TEXT,
               product TEXT, quantity INTEGER, amount REAL,
               PRIMARY KEY (source_file, row_id))"""
    )

    files = sorted(p for p in INCOMING.iterdir() if p.suffix in (".csv", ".json"))
    loaded_rows = rejected = 0
    for path in files:
        rows = read_rows(path)
        problems = validate(rows)
        if problems:
            # REVERT: nothing from this file is loaded; quarantine the file.
            shutil.move(str(path), QUARANTINE / path.name)
            rejected += 1
            print(f"✗ {path.name}: REJECTED ({len(problems)} problem(s), "
                  f"first: {problems[0]}) -> quarantined, no rows loaded")
            continue
        load(conn, path.name, rows)
        conn.commit()
        loaded_rows += len(rows)
        print(f"✓ {path.name}: {len(rows)} rows loaded")

    total = conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
    conn.close()
    elapsed = time.time() - started
    print(f"\nDone in {elapsed:.1f}s (SLA budget {SLA_SECONDS // 3600}h). "
          f"Files loaded this run added {loaded_rows} rows; "
          f"{rejected} file(s) quarantined; warehouse now holds {total} rows.")
    print("To reload a fixed file: correct it, move it back to data/incoming/, rerun.")


if __name__ == "__main__":
    main()
