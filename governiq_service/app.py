# governiq_service/app.py

from fastapi import FastAPI, Query, UploadFile, File
from fastapi.responses import HTMLResponse, Response, StreamingResponse, JSONResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from store import Store
from responder import answer_query
import time
from collections import Counter, defaultdict
import csv, io, json, os, time
import sqlite3
from contextlib import closing

# 👇 Mini-Patch: DB-Pfad dynamisch aus ENV lesen
DB_PATH = os.getenv("GOVERNIQ_DB_PATH", "governiq.db")

app = FastAPI(title="GovernIQ Service", version="0.1.2")

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")
# Optional: welche Datasets sollen Alerts auslösen
ALERT_DATASETS = set((os.getenv("ALERT_DATASETS", "").split(",")))

def should_alert(ds: str) -> bool:
    """Return True, wenn für dieses Dataset ein Alert gesendet werden soll"""
    return (not ALERT_DATASETS or ds in ALERT_DATASETS)


@app.post("/_selftest/slack")
def selftest_slack():
    if not SLACK_WEBHOOK:
        return {"ok": False, "reason": "SLACK_WEBHOOK not set in env"}
    try:
        resp = requests.post(SLACK_WEBHOOK, json={"text": "GovernIQ self-test ✅"}, timeout=8)
        return {"ok": resp.ok, "status": resp.status_code, "body": resp.text[:200]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


store = Store(db_path=DB_PATH)
store.init()
def _col_exists(cur, table, col) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())

def migrate_events_schema(db_path: str):
    with closing(sqlite3.connect(db_path, check_same_thread=False)) as con, closing(con.cursor()) as cur:
        # Falls Tabelle gar nicht existiert → minimal neu anlegen
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'")
        if not cur.fetchone():
            cur.execute("""
                CREATE TABLE events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER,
                    dataset TEXT,
                    contract_id TEXT,
                    owner TEXT,
                    status TEXT,
                    errors TEXT,
                    warnings TEXT,
                    violations TEXT
                )
            """)
            con.commit()
            return

        # Fehlende Spalten ergänzen (idempotent)
        needed = ["ts INTEGER", "dataset TEXT", "contract_id TEXT",
                  "owner TEXT", "status TEXT", "errors TEXT", "warnings TEXT", "violations TEXT"]
        for spec in needed:
            col = spec.split()[0]
            if not _col_exists(cur, "events", col):
                cur.execute(f"ALTER TABLE events ADD COLUMN {spec}")

        con.commit()

# 1) Schema migrieren
migrate_events_schema(DB_PATH)
# einmalige Index-Erstellung (idempotent)
try:
    with closing(sqlite3.connect(DB_PATH, check_same_thread=False)) as con, closing(con.cursor()) as cur:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_status ON events(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_owner ON events(owner)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_dataset ON events(dataset)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_contract ON events(contract_id)")
        con.commit()
except Exception as e:
    print("[DB][INDEX] skipped/failed:", e)


class Event(BaseModel):
    dataset: str
    contract_id: str
    owner: Optional[str] = None
    status: str
    errors: List[str] = []
    warnings: List[str] = []
    violations: List[Dict[str, Any]] = []

@app.post("/events")
def ingest_event(evt: Event):
    data = evt.model_dump()
    data["ts"] = data.get("ts") or int(time.time())
    store.insert_event(data)

    # --- DEBUG: log every event
    ds = data.get("dataset", "?")
    status = data.get("status", "?")
    owner = data.get("owner") or "n/a"
    print(f"[EVENT] dataset={ds} status={status} owner={owner}")

    # build violation summary
    vios = data.get("violations") or []
    vio_lines = []
    for v in vios:
        dim = v.get("dimension", "?")
        exp = v.get("expected", "?")
        act = v.get("actual") or v.get("actual_seconds") or "?"
        vio_lines.append(f"• {dim}: expected {exp}, actual {act}")
    vio_text = "\n".join(vio_lines) or "• (none)"

    # only alert on fails (and only selected datasets if filter set)
    if SLACK_WEBHOOK and status == "fail" and should_alert(ds):
        dash = os.getenv("GOVERNIQ_PUBLIC_URL", "")  # optional button link
        payload = {
          "blocks": [
            {"type":"header","text":{"type":"plain_text","text":"GovernIQ Alert"}},
            {"type":"section","fields":[
              {"type":"mrkdwn","text":f"*Dataset:*\n`{ds}`"},
              {"type":"mrkdwn","text":f"*Owner:*\n{owner}"},
              {"type":"mrkdwn","text":"*Status:*\n❌ fail"},
              {"type":"mrkdwn","text":f"*When:*\n{time.strftime('%Y-%m-%d %H:%M:%S')}"},
            ]},
            {"type":"section","text":{"type":"mrkdwn","text":f"*Violations*\n{vio_text}"}},
          ] + (
            [{"type":"actions","elements":[
              {"type":"button","text":{"type":"plain_text","text":"Open in GovernIQ"},
               "url": f"{dash}/?ds={ds}"}
            ]}] if dash else []
          )
        }
        try:
            r = requests.post(SLACK_WEBHOOK, json=payload, timeout=8)
            print(f"[SLACK] status={r.status_code} body={r.text[:200]}")
        except Exception as e:
            print(f"[SLACK][ERROR] {e}")

    return {"ok": True}

@app.get("/events")
def list_events(limit: int = 50):
    return store.fetch_events(limit=limit)
# governiq_app.py (oder wo deine FastAPI-App/DB-Helfer liegen)
app = FastAPI()

@app.get("/analytics/fails_per_day")
def api_fails_per_day(
    days: int = 30,
    owner: Optional[str] = None,
    q: Optional[str] = None,
    dimension: Optional[str] = None
):
    data = fails_per_day(days=days, owner=owner, q=q, dimension=dimension)
    return JSONResponse(content={"data": data})


def fails_per_day(days: int = 30, owner: Optional[str] = None, q: Optional[str] = None, dimension: Optional[str] = None):
    """
    Aggregiert Fail-Events pro Kalendertag aus SQLite.
    Annahme: events-Tabelle mit Spalten: ts (UNIX epoch), status, dataset, owner, contract_id, violations (TEXT/JSON).
    """
    filters = ["status = 'fail'", "ts >= strftime('%s','now','-{} days')".format(int(days))]
    params = []

    if owner:
        filters.append("owner = ?")
        params.append(owner)

    if q:
        like = f"%{q}%"
        filters.append("(dataset LIKE ? OR owner LIKE ? OR contract_id LIKE ?)")
        params.extend([like, like, like])

    if dimension:
        filters.append("CAST(violations AS TEXT) LIKE ?")
        params.append(f"%{dimension}%")

    where_sql = " AND ".join(filters)
    sql = f"""
      SELECT DATE(datetime(ts, 'unixepoch')) AS day, COUNT(*) AS fail_count
      FROM events
      WHERE {where_sql}
      GROUP BY day
      ORDER BY day ASC
    """

    with closing(sqlite3.connect(DB_PATH, check_same_thread=False)) as con, closing(con.cursor()) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [{"day": r[0], "fail_count": r[1]} for r in rows]

# --- Healthcheck (optional, praktisch) ---
@app.get("/health")
def health():
    return {"ok": True}

# --- Export: JSON ---
@app.get("/export.json")
def export_json(limit: int = 100_000, status: Optional[str] = None, owner: Optional[str] = None):
    events = store.fetch_events(limit=limit)
    if status:
        events = [e for e in events if e.get("status") == status]
    if owner:
        events = [e for e in events if (e.get("owner") or "") == owner]
    return Response(
        content=json.dumps(events, ensure_ascii=False, indent=2),
        media_type="application/json",
        headers={"Content-Disposition": 'attachment; filename="events.json"'}
    )

# --- Export: CSV ---
@app.get("/export.csv")
def export_csv(limit: int = 100_000, status: Optional[str] = None, owner: Optional[str] = None):
    events = store.fetch_events(limit=limit)
    if status:
        events = [e for e in events if e.get("status") == status]
    if owner:
        events = [e for e in events if (e.get("owner") or "") == owner]

    # CSV-Felder (einfach & robust)
    cols = ["dataset", "contract_id", "owner", "status", "ts", "errors", "warnings", "violations"]

    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    for e in events:
        row = {
            "dataset":   e.get("dataset", ""),
            "contract_id": e.get("contract_id", e.get("dataset", "")),
            "owner":     e.get("owner", ""),
            "status":    e.get("status", ""),
            "ts":        e.get("ts", ""),  # falls dein Store einen Zeitstempel speichert
            # JSON-Felder als String serialisieren
            "errors":    json.dumps(e.get("errors") or [], ensure_ascii=False),
            "warnings":  json.dumps(e.get("warnings") or [], ensure_ascii=False),
            "violations":json.dumps(e.get("violations") or [], ensure_ascii=False),
        }
        w.writerow(row)

    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="events.csv"'}
    )

# --- Import: JSON (Liste von Events) ---
from pydantic import BaseModel

class ImportPayload(BaseModel):
    events: List[Event]

@app.post("/import.json")
def import_json(payload: ImportPayload):
    count = 0
    for evt in payload.events:
        store.insert_event(evt.model_dump())
        count += 1
    return {"imported": count}

# --- Import: CSV (Upload) ---
@app.post("/import.csv")
async def import_csv(file: UploadFile = File(...)):
    content = await file.read()
    text = content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    count = 0

    def parse_json_field(row, name):
        val = row.get(name) or row.get(f"{name}_json")
        if not val:
            return []
        try:
            return json.loads(val)
        except Exception:
            # falls einfache Strings in der CSV stehen
            return [str(val)]

    for row in reader:
        ds = row.get("dataset") or row.get("Dataset") or row.get("name")
        status = row.get("status") or row.get("Status")
        if not ds or not status:
            # Zeilen ohne Mindestfelder überspringen
            continue
        evt = {
            "dataset": ds,
            "contract_id": row.get("contract_id") or ds,
            "owner": row.get("owner") or "",
            "status": status,
            "errors": parse_json_field(row, "errors"),
            "warnings": parse_json_field(row, "warnings"),
            "violations": parse_json_field(row, "violations"),
        }
        store.insert_event(evt)
        count += 1

    return {"imported": count}

@app.get("/_diag/llm")
def diag_llm():
    try:
        import openai as openai_pkg
        ver = getattr(openai_pkg, "__version__", "unknown")
    except Exception:
        ver = "not-importable"
    import os
    return {
        "LLM": os.getenv("LLM"),
        "LLM_MODE": os.getenv("LLM_MODE"),
        "OPENAI_MODEL": os.getenv("OPENAI_MODEL"),
        "openai_version_in_container": ver
    }

# add near your other routes, after `app = FastAPI(...)` and after `store` + imports exist
@app.get("/_selftest/llm")
def selftest_llm():
    try:
        events = store.fetch_events(limit=5)
        reply = answer_query("Summarize the latest dataset status in one short sentence.", events)
        return {"ok": True, "sample_answer": reply}
    except Exception as e:
        print(f"[LLM][SELFTEST][ERROR] {e}")
        return {"ok": False, "error": str(e)}

@app.get("/ask")
def ask(q: str = Query(..., description="Natural language question")):
    events = store.fetch_events(limit=200)
    reply = answer_query(q, events)
    return {"question": q, "answer": reply}

# NEW: simple analytics for last N hours (default 24)
@app.get("/analytics")
def analytics(hours: int = 24):
    now = time.time()
    since = now - hours * 3600
    evs = store.fetch_since(since_ts=since, limit=5000)

    total = len(evs)
    by_status = Counter(e.get("status") for e in evs)
    by_dataset = Counter(e.get("dataset") for e in evs if e.get("status") == "fail")

    # count violation dimensions (freshness, completeness, ...)
    by_dimension = Counter()
    for e in evs:
        if e.get("violations"):
            for v in e["violations"]:
                dim = v.get("dimension") or "unknown"
                by_dimension[dim] += 1

    # owners for failing datasets
    fail_owners = defaultdict(set)
    for e in evs:
        if e.get("status") == "fail":
            fail_owners[e.get("dataset")].add(e.get("owner") or "n/a")

    return {
        "window_hours": hours,
        "total_events": total,
        "status_counts": dict(by_status),
        "top_failing_datasets": [{"dataset": ds, "fails": c, "owners": sorted(list(fail_owners.get(ds, [])))} 
                                 for ds, c in by_dataset.most_common(10)],
        "violations_by_dimension": dict(by_dimension),
        "since_unix": since,
        "now_unix": now,
    }

# --- Dashboard (existing “/”) – extend JS to call /analytics ---
@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse("""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>GovernIQ – Event Feed & Ask</title>
<style>
  :root { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; color: #0f172a; }
  body { margin: 24px; }
  h1 { margin: 0 0 12px 0; font-size: 20px; }
  .grid { display: grid; grid-template-columns: 1.2fr 0.8fr; gap: 16px; align-items: start; }
  .row { display: grid; grid-template-columns: 1fr 360px; gap: 16px; }
  .card { border: 1px solid #e5e7eb; border-radius: 10px; padding: 12px 14px; background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,.04); }
  .feed { max-height: 60vh; overflow: auto; }
  .event { border-bottom: 1px dashed #e5e7eb; padding: 8px 0; }
  .badge { display: inline-block; font-size: 12px; padding: 2px 8px; border-radius: 999px; }
  .ok { background: #e9f8ee; color: #116c2f; }
  .fail { background: #fde8e8; color: #b42318; }
  .muted { color: #6b7280; font-size: 12px; }
  input[type="text"] { width: 100%; padding: 10px 12px; border: 1px solid #d1d5db; border-radius: 8px; }
  button { margin-top: 8px; width: 100%; padding: 10px 12px; border-radius: 8px; border: 0; background: #111827; color: #fff; cursor: pointer; }
  button:disabled { opacity: .6; cursor: default; }
  pre { white-space: pre-wrap; word-wrap: break-word; background: #f8fafc; padding: 10px; border-radius: 8px; }
  .hint { font-size: 12px; color:#64748b; margin-top:6px; }
  table { width:100%; border-collapse: collapse; }
  th, td { text-align:left; padding:6px 8px; border-bottom: 1px solid #e5e7eb; font-size: 13px; }
  .kpi { display:flex; gap:12px; flex-wrap:wrap; margin: 6px 0 10px 0}
  .kpi > div { background:#f8fafc; border:1px solid #e5e7eb; border-radius:10px; padding:8px 10px; font-size:12px; }
</style>
</head>
<body>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>                      
  <h1>GovernIQ – Event Feed, Analytics & Ask</h1>

  <div class="grid">
    <div class="card feed">
      <h2 style="margin-top:0;font-size:16px;">Recent Events</h2>
      <div id="events"></div>
    </div>

    <div class="card">
      <h2 style="margin-top:0;font-size:16px;">Ask</h2>
      <input id="q" type="text" placeholder="e.g., Why did dataset sales_orders_v1 fail?" />
      <button id="askBtn">Ask</button>
      <div class="hint">Tip: Try <code>Why did dataset sales_orders_v1 fail?</code></div>
      <div id="answer" style="margin-top:10px;"></div>
    </div>
  </div>

  <div class="card" style="margin-top:16px;">
    <div style="display:flex; align-items:center; gap:12px;">
      <h2 style="margin:0; font-size:16px;">Analytics (last <span id="winH">24</span>h)</h2>
      <select id="hours">
        <option value="6">6h</option>
        <option value="12">12h</option>
        <option value="24" selected>24h</option>
        <option value="72">72h</option>
        <option value="168">7d</option>
      </select>
    </div>
    <div class="kpi">
      <div>Total events: <b id="kpiTotal">-</b></div>
      <div>Status: <b id="kpiStatus">-</b></div>
      <div>Violations: <b id="kpiDims">-</b></div>
    </div>
    <div class="row" style="grid-template-columns: 1fr 1fr;">
      <div>
        <h3 style="font-size:14px;margin:6px 0">Top failing datasets</h3>
        <table id="tblFail"><thead><tr><th>Dataset</th><th>Fails</th><th>Owners</th></tr></thead><tbody></tbody></table>
      </div>
      <div>
        <h3 style="font-size:14px;margin:6px 0">Violations by dimension</h3>
        <table id="tblDims"><thead><tr><th>Dimension</th><th>Count</th></tr></thead><tbody></tbody></table>
      </div>
    <h3 style="font-size:14px;margin:10px 0 6px;">Fails pro Tag</h3>
    <div style="height:260px; border:1px solid #eee; border-radius:8px; padding:8px;">
      <canvas id="failsPerDay" style="width:100%; height:240px;"></canvas>                    
    </div>
  </div>

<script>
  async function loadEvents() {
    try {
      const res = await fetch('/events?limit=50');
      const data = await res.json();
      const el = document.getElementById('events');
      el.innerHTML = '';
      data.forEach(ev => {
        const s = ev.status === 'pass' ? 'ok' : 'fail';
        const when = new Date(((ev.ts || ev._ts || 0) * 1000)).toLocaleString();
        const vio = (ev.violations || []).map(v => {
          if (v.dimension === 'freshness') {
            return `freshness: actual ${v.actual_seconds || '?'}s > ${v.expected}`;
          } else if (v.dimension === 'completeness') {
            return `completeness: actual ${v.actual || '?'} vs ${v.expected}`;
          } else {
            return JSON.stringify(v);
          }
        }).join(' | ');
        const errors = (ev.errors || []).join('; ');
        const div = document.createElement('div');
        div.className = 'event';
        div.innerHTML = `
          <div><span class="badge ${s}">${ev.status}</span> <strong>${ev.dataset}</strong> <span class="muted">• ${when}</span></div>
          <div class="muted">owner: ${ev.owner || 'n/a'}</div>
          ${vio ? `<div>violations: ${vio}</div>` : ``}
          ${errors ? `<div class="muted">errors: ${errors}</div>` : ``}
        `;
        el.appendChild(div);
      });
    } catch(e) { console.error(e); }
  }

  async function askQ() {
    const btn = document.getElementById('askBtn');
    const q   = document.getElementById('q').value;
    const ans = document.getElementById('answer');
    if (!q) return;
    btn.disabled = true;
    ans.innerHTML = '<div class="muted">Thinking…</div>';
    try {
      const res = await fetch('/ask?q=' + encodeURIComponent(q));
      const data = await res.json();
      ans.innerHTML = `<pre>${data.answer}</pre>`;
    } catch (e) {
      ans.innerHTML = '<div class="muted">Error. Check console.</div>';
      console.error(e);
    } finally {
      btn.disabled = false;
    }
  }

  // NEW: analytics loader
  async function loadAnalytics() {
    const hours = document.getElementById('hours').value;
    const res = await fetch('/analytics?hours=' + hours);
    const data = await res.json();
    document.getElementById('winH').textContent = hours;
    document.getElementById('kpiTotal').textContent = data.total_events;

    const sc = data.status_counts || {};
    document.getElementById('kpiStatus').textContent = Object.entries(sc).map(([k,v]) => `${k}:${v}`).join(', ') || '-';

    const dims = data.violations_by_dimension || {};
    document.getElementById('kpiDims').textContent = Object.entries(dims).map(([k,v]) => `${k}:${v}`).join(', ') || '-';

    const tbodyFail = document.querySelector('#tblFail tbody');
    tbodyFail.innerHTML = '';
    (data.top_failing_datasets || []).forEach(r => {
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${r.dataset}</td><td>${r.fails}</td><td>${(r.owners||[]).join(', ')}</td>`;
      tbodyFail.appendChild(tr);
    });

    const tbodyDims = document.querySelector('#tblDims tbody');
    tbodyDims.innerHTML = '';
    Object.entries(dims).sort((a,b)=>b[1]-a[1]).forEach(([dim, cnt]) => {
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${dim}</td><td>${cnt}</td>`;
      tbodyDims.appendChild(tr);
    });
  }
  let _trendChart = null;

  async function loadFailsTrend() {
    const hoursSel = document.getElementById('hours');
    const hours = Number(hoursSel.value || '24');
    // days aus hours ableiten (mindestens 1)
    const days = Math.max(1, Math.ceil(hours / 24));

    // vorhandene Filter aus der URL übernehmen (optional)
    const params = new URLSearchParams(window.location.search);
    if (!params.get('days')) params.set('days', String(days)); else params.set('days', String(days));
    // mappe evtl. 'search' -> 'q'
    if (params.get('search') && !params.get('q')) params.set('q', params.get('search'));

    const url = '/analytics/fails_per_day?' + params.toString();
    const res = await fetch(url);
    const { data } = await res.json();

    const labels = data.map(d => d.day);
    const values = data.map(d => d.fail_count);

    const ctx = document.getElementById('failsPerDay').getContext('2d');
    if (_trendChart) {
      _trendChart.data.labels = labels;
      _trendChart.data.datasets[0].data = values;
      _trendChart.update();
    } else {
      _trendChart = new Chart(ctx, {
        type: 'line',
        data: {
          labels,
          datasets: [{ label: 'Failing Events pro Tag', data: values, tension: 0.25, fill: false }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            x: { title: { display: true, text: 'Tag' } },
            y: { beginAtZero: true, ticks: { precision: 0 }, title: { display: true, text: 'Fails' } }
          }
        }
      });
    }
  }
 
  document.getElementById('askBtn').addEventListener('click', askQ);
  document.getElementById('q').addEventListener('keydown', (e) => { if (e.key === 'Enter') askQ(); });
  document.getElementById('hours').addEventListener('change', loadAnalytics);

  loadEvents();
  loadAnalytics();
  loadFailsTrend();                      
  setInterval(loadEvents, 5000);
  setInterval(loadAnalytics, 15000);
</script>
</body>
</html>
""")
