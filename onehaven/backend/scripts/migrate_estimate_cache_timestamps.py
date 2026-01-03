# scripts/migrate_estimate_cache_timestamps.py
import sqlite3

DB_PATH = "haven.db"

def col_exists(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    cur.execute(f"PRAGMA table_info({table});")
    return any(row[1] == col for row in cur.fetchall())

def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Make sure table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='estimate_cache';")
    if not cur.fetchone():
        raise SystemExit("estimate_cache table does not exist. Run scripts/init_db.py first.")

    changed = False

    if not col_exists(cur, "estimate_cache", "created_at"):
        cur.execute("ALTER TABLE estimate_cache ADD COLUMN created_at TEXT DEFAULT (datetime('now'));")
        print("Added estimate_cache.created_at")
        changed = True
    else:
        print("estimate_cache.created_at already exists")

    if not col_exists(cur, "estimate_cache", "updated_at"):
        cur.execute("ALTER TABLE estimate_cache ADD COLUMN updated_at TEXT DEFAULT (datetime('now'));")
        print("Added estimate_cache.updated_at")
        changed = True
    else:
        print("estimate_cache.updated_at already exists")

    if changed:
        conn.commit()
        print("OK: migration applied.")
    else:
        print("OK: nothing to do.")

    conn.close()

if __name__ == "__main__":
    main()
