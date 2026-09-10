"""
Wait for an incoming CSV, count its data rows, and report the count downstream.

Airflow 3 authoring (``airflow.sdk``), TaskFlow API, idempotent tasks.

    wait_for_data_csv (FileSensor) -> count_rows -> report_row_count
                                          |___ XCom ___^
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import Param, dag, get_current_context, task

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}


@dag(
    dag_id="csv_row_count_pipeline",
    description="Sense /data/data.csv, count its rows, push the count via XCom",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["sensor", "taskflow", "csv"],
    params={
        "csv_path": Param(
            "/data/data.csv",
            type="string",
            description="Absolute path of the CSV to wait for and count",
        ),
    },
)
def csv_row_count_pipeline():
    # Waits for the file to appear. `reschedule` releases the worker slot between
    # pokes instead of blocking it, and the sensor only reads directory state, so
    # re-running it has no side effects.
    wait_for_data_csv = FileSensor(
        task_id="wait_for_data_csv",
        filepath="{{ params.csv_path }}",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 60,
        mode="reschedule",
    )

    @task
    def count_rows() -> int:
        """Count data rows (excluding the header). Read-only, so re-running is safe."""
        import csv

        csv_path = get_current_context()["params"]["csv_path"]

        with open(csv_path, newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            next(reader, None)  # drop the header
            row_count = sum(1 for _ in reader)

        print(f"Counted {row_count} data rows in {csv_path}")
        return row_count

    @task
    def report_row_count(row_count: int) -> None:
        """Consume the count pulled from XCom and write it to the task log."""
        csv_path = get_current_context()["params"]["csv_path"]
        print(f"{csv_path} contains {row_count} data rows.")

    row_count = count_rows()
    wait_for_data_csv >> row_count
    report_row_count(row_count)


csv_row_count_pipeline()