from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN = "postgres_default"
default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=10)}


def build_user_activity(**_):
    pg = PostgresHook(postgres_conn_id=PG_CONN)
    pg.run("TRUNCATE dm.user_activity", autocommit=True)
    pg.run(
        """
        WITH top_device AS (
            SELECT report_date, user_id, device_type
            FROM (
                SELECT
                    start_time::date AS report_date,
                    user_id,
                    device_type,
                    ROW_NUMBER() OVER (
                        PARTITION BY start_time::date, user_id
                        ORDER BY COUNT(*) DESC
                    ) AS rn
                FROM raw.user_sessions
                WHERE end_time IS NOT NULL AND end_time > start_time
                GROUP BY start_time::date, user_id, device_type
            ) t
            WHERE rn = 1
        )
        INSERT INTO dm.user_activity
            (report_date, user_id, total_sessions, total_duration_min,
             avg_duration_min, unique_pages, total_actions, most_used_device)
        SELECT
            s.start_time::date,
            s.user_id,
            COUNT(*),
            ROUND(SUM(EXTRACT(EPOCH FROM (s.end_time - s.start_time)) / 60)::numeric, 2),
            ROUND(AVG(EXTRACT(EPOCH FROM (s.end_time - s.start_time)) / 60)::numeric, 2),
            SUM(array_length(s.pages_visited, 1)),
            SUM(array_length(s.actions, 1)),
            td.device_type
        FROM raw.user_sessions s
        JOIN top_device td ON td.report_date = s.start_time::date AND td.user_id = s.user_id
        WHERE s.end_time IS NOT NULL AND s.end_time > s.start_time
        GROUP BY s.start_time::date, s.user_id, td.device_type
        """,
        autocommit=True,
    )
    print("user_activity done")


def build_support_efficiency(**_):
    pg = PostgresHook(postgres_conn_id=PG_CONN)
    pg.run("TRUNCATE dm.support_efficiency", autocommit=True)
    pg.run(
        """
        INSERT INTO dm.support_efficiency
            (report_date, issue_type, status, ticket_count, avg_resolution_hrs, max_resolution_hrs)
        SELECT
            created_at::date,
            COALESCE(issue_type, 'unknown'),
            COALESCE(status, 'unknown'),
            COUNT(*),
            ROUND(AVG(EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600)::numeric, 2),
            ROUND(MAX(EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600)::numeric, 2)
        FROM raw.support_tickets
        WHERE updated_at >= created_at
        GROUP BY created_at::date, issue_type, status
        """,
        autocommit=True,
    )
    print("support_efficiency done")


def check_quality(**_):
    pg = PostgresHook(postgres_conn_id=PG_CONN)
    for table in ("dm.user_activity", "dm.support_efficiency"):
        n = pg.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        if n == 0:
            raise ValueError(f"{table} пустая")
        print(f"{table}: {n} rows OK")


with DAG(
    dag_id="build_analytics_marts",
    default_args=default_args,
    start_date=datetime(2026, 1, 2),
    schedule_interval="0 3 * * *",
    catchup=False,
    tags=["analytics"],
) as dag:
    t_ua = PythonOperator(task_id="user_activity", python_callable=build_user_activity)
    t_se = PythonOperator(task_id="support_efficiency", python_callable=build_support_efficiency)
    t_qc = PythonOperator(task_id="quality_check", python_callable=check_quality)

    [t_ua, t_se] >> t_qc
