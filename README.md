# Global ETL Data Pipeline (Demo)

![CI](https://github.com/rudhrestam/global-etl-data-pipeline/actions/workflows/ci.yml/badge.svg) ![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

A batch ETL pipeline with the part most demos skip: a **data-quality layer that reverts bad data instead of loading it**. Files arrive in mixed formats, every row is validated, clean files load into a SQLite warehouse — and if a file contains bad rows, none of it loads.

> This is a representative demo of patterns used in production systems, built with sample files and original code. It contains no employer code, data, or internal details. All sample data is fictional.

## How it works

```
data/incoming/ ──► extract (CSV + JSON) ──► validate every row
                                                │
                          ┌─── any problem ─────┤
                          │                     └── clean ──► load ──► warehouse.db (SQLite)
                          ▼
              REVERT: load nothing from that file
              quarantine the file ──► fix it ──► drop it back in ──► rerun (idempotent reload)
```

- **Extract:** every `.csv` / `.json` file in `data/incoming/` is read with a format-specific reader behind one interface.
- **Validate:** required fields, types, and basic sanity rules (no negative quantities or amounts) are checked for *every* row before anything loads.
- **Load:** clean files load into SQLite keyed by `(source_file, row_id)` with `INSERT OR REPLACE` — reruns and reloads never create duplicates.
- **Revert / reload:** a file with any invalid row is moved to `data/quarantine/` untouched and loads zero rows. Correct it, move it back, rerun. Partial bad data can never reach the warehouse.

## See the quality layer work

The sample data includes one deliberately corrupted file:

- `sales_2026-01-01.csv` — clean (5 rows)
- `sales_2026-01-02.json` — clean (4 rows)
- `sales_2026-01-03.csv` — **corrupted**: a negative quantity and a non-numeric amount → rejected and quarantined, 0 rows loaded

## Run it

```bash
python pipeline.py        # no dependencies, Python 3.10+
sqlite3 warehouse.db "SELECT COUNT(*), SUM(amount) FROM sales;"
```

An optional Airflow DAG in `dags/etl_dag.py` shows how the same steps map to a scheduled pipeline.

## Scheduling, parallelism, and SLA thinking

Production versions of this pattern run on a fixed nightly window, and three ideas carry over directly:

- **Scheduling:** the run is a single repeatable job (cron / Airflow), not a manual process — same steps, same order, every night.
- **Parallel processing:** files are independent, so extraction and validation parallelize naturally (in the DAG version, one task per file). The load step stays serialized to keep the warehouse consistent.
- **SLA thinking:** the pipeline measures its own elapsed time against a stated window budget and reports it every run. An SLA you don't measure is a hope, not a commitment.

## Tech stack

Python 3.10+ · SQLite · CSV/JSON · optional Apache Airflow DAG

## What it demonstrates

- Multi-format batch ETL behind a single pipeline interface
- Validate-before-load design with all-or-nothing file semantics
- Revert/quarantine/reload data-quality handling
- Idempotent loads (safe reruns by construction)
- Self-measured runtime against an SLA budget
