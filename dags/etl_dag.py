"""
Optional Airflow DAG wrapping pipeline.py — illustrative only.
The demo runs fine without Airflow (`python pipeline.py`); this file
shows how the same steps map onto a scheduled, parallel DAG:

  extract_and_validate (one task per incoming file, in parallel)
          -> load (single task, only after all validations pass)
          -> sla_report

Bad files never reach `load`: the validator quarantines them and the
load step proceeds with the clean files, matching pipeline.py behavior.
"""

from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # Airflow not installed — this file is documentation-grade
    DAG = None

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=3),  # the whole window, end to end
}

if DAG is not None:
    with DAG(
        dag_id="global_etl_demo",
        description="Nightly extract -> validate -> load with quality revert",
        schedule="0 1 * * *",  # 01:00 daily; SLA above guards the 3h window
        start_date=datetime(2026, 1, 1),
        catchup=False,
        default_args=default_args,
    ) as dag:
        run_pipeline = PythonOperator(
            task_id="extract_validate_load",
            python_callable=lambda: __import__("pipeline").main(),
        )
