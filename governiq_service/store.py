import sqlite3
import json
import time
from typing import List, Dict, Any


class Store:
    def __init__(self, db_path: str = "governiq.db"):
        self.db_path = db_path

    def init(self) -> None:
        """Initialize the events table if it doesn't exist."""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL,
                    payload TEXT
                )
                """
            )
            conn.commit()

    def insert_event(self, payload: Dict[str, Any]) -> None:
        """Insert a single event (JSON payload + timestamp)."""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO events (ts, payload) VALUES (?, ?)",
                (time.time(), json.dumps(payload)),
            )
            conn.commit()

    def fetch_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Fetch latest events (most recent first)."""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT ts, payload FROM events ORDER BY id DESC LIMIT ?",
                (limit,),
            )
            rows = cur.fetchall()
            out: List[Dict[str, Any]] = []
            for ts, payload in rows:
                d = json.loads(payload)
                d["_ts"] = ts
                out.append(d)
            return out

    def fetch_since(self, since_ts: float, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Fetch events stored at or after `since_ts` (unix epoch seconds),
        ordered newest first, up to `limit`.
        """
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT ts, payload FROM events WHERE ts >= ? ORDER BY id DESC LIMIT ?",
                (since_ts, limit),
            )
            rows = cur.fetchall()
            out: List[Dict[str, Any]] = []
            for ts, payload in rows:
                d = json.loads(payload)
                d["_ts"] = ts
                out.append(d)
            return out
