# scripts/migrate_add_estimated_at.py
import sqlite3
from pathlib import Path

DB_PATH = Path("onehaven.db")  # change if your sqlite path differs


CREATE_ESTIMATE_CACHE_SQL = """
CREATE TABLE IF NOT EXISTS estimate_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    value REAL,
    source TEXT DEFAULT 'unknown',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    estimated_at TEXT DEFAULT (datetime('now')),
    raw_json TEXT,
    UNIQUE(property_id, kind)
);
"""


def table_exists(cur: sqlite3.Cursor, table_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    )
    return cur.fetchone() is not None


def column_exists(cur: sqlite3.Cursor, table_name: str, column_name: str) -> bool:
    cur.execute(f"PRAGMA table_info({table_name})")
    cols = [row[1] for row in cur.fetchall()]  # row[1] = column name
    return column_name in cols


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # If the table doesn't exist, create it (so ALTER won't explode)
    if not table_exists(cur, "estimate_cache"):
        cur.executescript(CREATE_ESTIMATE_CACHE_SQL)
        print("Created estimate_cache table (it was missing).")

    # Add column if missing (older DBs)
    if not column_exists(cur, "estimate_cache", "estimated_at"):
        cur.execute("ALTER TABLE estimate_cache ADD COLUMN estimated_at TEXT")
        print("Added estimate_cache.estimated_at")
    else:
        print("estimate_cache.estimated_at already exists")

    # Backfill nulls
    cur.execute(
        "UPDATE estimate_cache SET estimated_at = COALESCE(estimated_at, datetime('now'))"
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
