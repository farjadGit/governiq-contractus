import sqlite3, json, time

class Store:
    def __init__(self, db_path: str = "governiq.db"):
        self.db_path = db_path

    def init(self):
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, payload TEXT)"
            )
            conn.commit()

    def insert_event(self, payload: dict):
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO events (ts, payload) VALUES (?, ?)",
                (time.time(), json.dumps(payload)),
            )
            conn.commit()

    def fetch_events(self, limit: int = 50):
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT ts, payload FROM events ORDER BY id DESC LIMIT ?", (limit,))
            rows = cur.fetchall()
            out = []
            for ts, payload in rows:
                d = json.loads(payload)
                d["_ts"] = ts
                out.append(d)
            return out
