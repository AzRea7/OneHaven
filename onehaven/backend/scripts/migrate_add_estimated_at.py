# scripts/migrate_add_estimated_at.py
import sqlite3
from pathlib import Path

DB_PATH = Path("onehaven.db")  # change if your sqlite path differs

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # SQLite: ADD COLUMN works, but NOT NULL requires default. We'll add nullable then backfill.
    try:
        cur.execute("ALTER TABLE estimate_cache ADD COLUMN estimated_at TEXT")
        print("Added estimate_cache.estimated_at")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            print("estimate_cache.estimated_at already exists")
        else:
            raise

    # Backfill nulls to current timestamp ISO
    cur.execute(
        "UPDATE estimate_cache SET estimated_at = COALESCE(estimated_at, datetime('now'))"
    )

    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
