from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

MONGO_DB = "ecommerce"
PG_CONN = "postgres_default"

default_args = {"owner": "airflow", "retries": 2, "retry_delay": timedelta(minutes=5)}


def _mongo():
    from pymongo import MongoClient
    return MongoClient("mongodb://mongo:27017/")[MONGO_DB]


def _executemany(pg, sql, rows):
    conn = pg.get_conn()
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()


def replicate_user_sessions(**_):
    db = _mongo()
    pg = PostgresHook(postgres_conn_id=PG_CONN)
    rows, seen = [], set()
    for doc in db["UserSessions"].find():
        sid, st = doc.get("session_id"), doc.get("start_time")
        if not sid or not st or (sid, st) in seen:
            continue
        seen.add((sid, st))
        dev = doc.get("device") or {}
        if isinstance(dev, set):
            dev = {}
        rows.append((
            sid, doc.get("user_id"), st, doc.get("end_time"),
            doc.get("pages_visited") or [], dev.get("type"), dev.get("os"),
            doc.get("actions") or [],
        ))
    _executemany(pg,
        """INSERT INTO raw.user_sessions
               (session_id, user_id, start_time, end_time, pages_visited, device_type, device_os, actions)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (session_id, start_time) DO UPDATE SET
               end_time=EXCLUDED.end_time, pages_visited=EXCLUDED.pages_visited,
               device_type=EXCLUDED.device_type, device_os=EXCLUDED.device_os,
               actions=EXCLUDED.actions, loaded_at=NOW()""",
        rows,
    )
    print(f"user_sessions: {len(rows)}")


def replicate_event_logs(**_):
    db = _mongo()
    pg = PostgresHook(postgres_conn_id=PG_CONN)
    rows, seen = [], set()
    for doc in db["EventLogs"].find():
        eid, ts = doc.get("event_id"), doc.get("timestamp")
        if not eid or not ts or (eid, ts) in seen:
            continue
        seen.add((eid, ts))
        det = doc.get("details") or {}
        if isinstance(det, set):
            det = {}
        rows.append((eid, ts, doc.get("event_type"), det.get("page"), det.get("product_id"), det.get("user_id")))
    _executemany(pg,
        """INSERT INTO raw.event_logs (event_id, ts, event_type, page, product_id, user_id)
           VALUES (%s,%s,%s,%s,%s,%s)
           ON CONFLICT (event_id, ts) DO NOTHING""",
        rows,
    )
    print(f"event_logs: {len(rows)}")


def replicate_support_tickets(**_):
    db = _mongo()
    pg = PostgresHook(postgres_conn_id=PG_CONN)
    rows, seen = [], set()
    for doc in db["SupportTickets"].find():
        tid = doc.get("ticket_id")
        if not tid or tid in seen:
            continue
        seen.add(tid)
        rows.append((
            tid, doc.get("user_id"), doc.get("status"), doc.get("issue_type"),
            doc.get("created_at"), doc.get("updated_at"), len(doc.get("messages") or []),
        ))
    _executemany(pg,
        """INSERT INTO raw.support_tickets
               (ticket_id, user_id, status, issue_type, created_at, updated_at, message_count)
           VALUES (%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (ticket_id) DO UPDATE SET
               status=EXCLUDED.status, updated_at=EXCLUDED.updated_at,
               message_count=EXCLUDED.message_count, loaded_at=NOW()""",
        rows,
    )
    print(f"support_tickets: {len(rows)}")


def replicate_user_recommendations(**_):
    db = _mongo()
    pg = PostgresHook(postgres_conn_id=PG_CONN)
    rows, seen = [], set()
    for doc in db["UserRecommendations"].find():
        uid = doc.get("user_id")
        if not uid or uid in seen:
            continue
        seen.add(uid)
        rows.append((uid, doc.get("recommended_products") or [], doc.get("last_updated")))
    _executemany(pg,
        """INSERT INTO raw.user_recommendations (user_id, recommended_products, last_updated)
           VALUES (%s,%s,%s)
           ON CONFLICT (user_id) DO UPDATE SET
               recommended_products=EXCLUDED.recommended_products,
               last_updated=EXCLUDED.last_updated, loaded_at=NOW()""",
        rows,
    )
    print(f"user_recommendations: {len(rows)}")


def replicate_moderation_queue(**_):
    db = _mongo()
    pg = PostgresHook(postgres_conn_id=PG_CONN)
    rows, seen = [], set()
    for doc in db["ModerationQueue"].find():
        rid = doc.get("review_id")
        if not rid or rid in seen:
            continue
        seen.add(rid)
        try:
            rating = int(doc["rating"]) if doc.get("rating") is not None else None
        except (ValueError, TypeError):
            rating = None
        rows.append((
            rid, doc.get("user_id"), doc.get("product_id"),
            (doc.get("review_text") or "").strip(), rating,
            doc.get("moderation_status", "pending"),
            [f for f in (doc.get("flags") or []) if f],
            doc.get("submitted_at"),
        ))
    _executemany(pg,
        """INSERT INTO raw.moderation_queue
               (review_id, user_id, product_id, review_text, rating, moderation_status, flags, submitted_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (review_id) DO UPDATE SET
               moderation_status=EXCLUDED.moderation_status, loaded_at=NOW()""",
        rows,
    )
    print(f"moderation_queue: {len(rows)}")


with DAG(
    dag_id="mongo_to_postgres_replication",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 */6 * * *",
    catchup=False,
    tags=["replication"],
) as dag:
    t1 = PythonOperator(task_id="user_sessions", python_callable=replicate_user_sessions)
    t2 = PythonOperator(task_id="event_logs", python_callable=replicate_event_logs)
    t3 = PythonOperator(task_id="support_tickets", python_callable=replicate_support_tickets)
    t4 = PythonOperator(task_id="user_recommendations", python_callable=replicate_user_recommendations)
    t5 = PythonOperator(task_id="moderation_queue", python_callable=replicate_moderation_queue)

    [t1, t2, t3, t4, t5]
