# governiq_service/app.py

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from store import Store
from responder import answer_query
import time
from collections import Counter, defaultdict
import os, requests, time

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
        const when = new Date((ev._ts || 0)*1000).toLocaleString();
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

  document.getElementById('askBtn').addEventListener('click', askQ);
  document.getElementById('q').addEventListener('keydown', (e) => { if (e.key === 'Enter') askQ(); });
  document.getElementById('hours').addEventListener('change', loadAnalytics);

  loadEvents();
  loadAnalytics();
  setInterval(loadEvents, 5000);
  setInterval(loadAnalytics, 15000);
</script>
</body>
</html>
""")
