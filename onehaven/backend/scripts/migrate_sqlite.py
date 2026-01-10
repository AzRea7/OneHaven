# scripts/migrate_sqlite.py
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


DEFAULT_CANDIDATES = [
    "haven.db",
    "onehaven.db",
    "app.db",
]


def has_table(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (table,),
    ).fetchone()
    return row is not None


def has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    if not has_table(conn, table):
        return False
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return any(r[1] == col for r in rows)  # r[1] = column name


def add_column(conn: sqlite3.Connection, table: str, col: str, ddl_type: str) -> None:
    print(f"[migrate] adding {table}.{col} {ddl_type}")
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl_type};")


def resolve_db_path(explicit: str | None) -> str:
    if explicit:
        return explicit

    for p in DEFAULT_CANDIDATES:
        if Path(p).exists():
            return p

    # fall back to first candidate (it will error with a helpful message)
    return DEFAULT_CANDIDATES[0]


def main(db_path: str) -> None:
    if not Path(db_path).exists():
        raise SystemExit(
            f"DB not found: {Path(db_path).resolve()}\n"
            f"Pass --db PATH, or create one via: python scripts/init_db.py"
        )

    conn = sqlite3.connect(db_path)
    try:
        # -------------------------
        # leads (schema drift)
        # -------------------------
        if has_table(conn, "leads"):
            # Existing migrations already handled some of these in older versions,
            # but we keep this idempotent and additive.
            if not has_column(conn, "leads", "explain_json"):
                add_column(conn, "leads", "explain_json", "TEXT")
            if not has_column(conn, "leads", "reasons_json"):
                add_column(conn, "leads", "reasons_json", "TEXT")
            if not has_column(conn, "leads", "raw_json"):
                add_column(conn, "leads", "raw_json", "TEXT")

            # Optional: if your DB was created super early, you may also be missing:
            if not has_column(conn, "leads", "created_at"):
                add_column(conn, "leads", "created_at", "TEXT")
            if not has_column(conn, "leads", "updated_at"):
                add_column(conn, "leads", "updated_at", "TEXT")
        else:
            print("[migrate] table 'leads' does not exist yet (skipping leads drift fixes)")

        # -------------------------
        # job_runs (schema drift)
        # -------------------------
        if has_table(conn, "job_runs"):
            if not has_column(conn, "job_runs", "meta_json"):
                add_column(conn, "job_runs", "meta_json", "TEXT NOT NULL DEFAULT '{}'")
            if not has_column(conn, "job_runs", "summary_json"):
                add_column(conn, "job_runs", "summary_json", "TEXT")
            if not has_column(conn, "job_runs", "error"):
                add_column(conn, "job_runs", "error", "TEXT")
            if not has_column(conn, "job_runs", "detail"):
                add_column(conn, "job_runs", "detail", "TEXT")

        # -------------------------
        # estimate_cache (percentiles/timestamps drift)
        # -------------------------
        if has_table(conn, "estimate_cache"):
            if not has_column(conn, "estimate_cache", "p10"):
                add_column(conn, "estimate_cache", "p10", "REAL")
            if not has_column(conn, "estimate_cache", "p50"):
                add_column(conn, "estimate_cache", "p50", "REAL")
            if not has_column(conn, "estimate_cache", "p90"):
                add_column(conn, "estimate_cache", "p90", "REAL")
            if not has_column(conn, "estimate_cache", "estimated_at"):
                add_column(conn, "estimate_cache", "estimated_at", "DATETIME")
            if not has_column(conn, "estimate_cache", "created_at"):
                add_column(conn, "estimate_cache", "created_at", "TEXT")
            if not has_column(conn, "estimate_cache", "updated_at"):
                add_column(conn, "estimate_cache", "updated_at", "TEXT")

        conn.commit()
        print("[migrate] done")
    finally:
        conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None, help="Path to sqlite db (defaults: haven.db/onehaven.db/app.db)")
    args = ap.parse_args()

    db_path = resolve_db_path(args.db)
    main(db_path)
