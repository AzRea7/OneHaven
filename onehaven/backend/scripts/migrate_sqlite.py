# scripts/migrate_sqlite.py
import sqlite3

DB = "haven.db"


def has_table(conn, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (table,),
    ).fetchone()
    return row is not None


def has_column(conn, table: str, col: str) -> bool:
    if not has_table(conn, table):
        return False
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return any(r[1] == col for r in rows)


def add_column(conn, table: str, col: str, ddl_type: str) -> None:
    print(f"[migrate] adding {table}.{col} {ddl_type}")
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl_type};")


def main(db_path: str = DB) -> None:
    conn = sqlite3.connect(db_path)
    try:
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
        # estimate_cache (A: percentiles)
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

        # Example drift (keep if your code references it)
        if has_table(conn, "leads"):
            if not has_column(conn, "leads", "rehab_estimate"):
                add_column(conn, "leads", "rehab_estimate", "REAL")

        conn.commit()
        print("[migrate] done")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
