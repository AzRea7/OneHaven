# scripts/migrate_sqlite.py
import sqlite3

DB = "haven.db"

def has_column(conn, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return any(r[1] == col for r in rows)

def add_column(conn, table: str, col: str, ddl_type: str) -> None:
    print(f"[migrate] adding {table}.{col} {ddl_type}")
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl_type};")

def main():
    conn = sqlite3.connect(DB)
    try:
        # leads
        if not has_column(conn, "leads", "rehab_estimate"):
            add_column(conn, "leads", "rehab_estimate", "REAL")

        # job_runs (since you added it manually already, this will no-op if present)
        if not has_column(conn, "job_runs", "summary_json"):
            add_column(conn, "job_runs", "summary_json", "TEXT")

        conn.commit()
        print("[migrate] done")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
