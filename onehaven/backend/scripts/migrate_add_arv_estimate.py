from __future__ import annotations

import sqlite3
from pathlib import Path


def main() -> None:
    # Adjust to your actual sqlite path if different
    db_path = Path("haven.db")
    if not db_path.exists():
        # try common alt
        db_path = Path("app.db")
    if not db_path.exists():
        raise SystemExit("Could not find sqlite db file (haven.db or app.db). Set path in script.")

    con = sqlite3.connect(str(db_path))
    cur = con.cursor()

    # Check if column exists
    cur.execute("PRAGMA table_info(leads);")
    cols = {row[1] for row in cur.fetchall()}
    if "arv_estimate" in cols:
        print("leads.arv_estimate already exists. Nothing to do.")
        return

    # Add the column (nullable)
    cur.execute("ALTER TABLE leads ADD COLUMN arv_estimate REAL;")
    con.commit()
    con.close()
    print("Added leads.arv_estimate REAL")


if __name__ == "__main__":
    main()
