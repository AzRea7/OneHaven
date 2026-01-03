# scripts/migrate_add_job_runs_summary_json.py
import sqlite3
from pathlib import Path

DB = Path("onehaven.db")  # change this AFTER you confirm the real DB path

def main() -> None:
    if not DB.exists():
        raise SystemExit(f"DB not found: {DB.resolve()}")

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(job_runs);")
    cols = {row[1] for row in cur.fetchall()}
    if "summary_json" in cols:
        print("job_runs.summary_json already exists")
        return

    # SQLite supports ADD COLUMN
    cur.execute("ALTER TABLE job_runs ADD COLUMN summary_json TEXT;")
    conn.commit()
    print("OK: added job_runs.summary_json")

if __name__ == "__main__":
    main()
