import os
import re
from typing import List, Dict, Any

# --- Feature toggles ---
USE_LLM   = os.getenv("LLM", "off").lower() in ("1", "true", "on", "yes")
LLM_MODE  = os.getenv("LLM_MODE", "openai").lower()  # "openai" | "llama"
# Defaults (can be overridden via env)
OPENAI_MODEL  = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OLLAMA_MODEL  = os.getenv("OLLAMA_MODEL", "llama3")
OLLAMA_URL    = os.getenv("OLLAMA_URL", "http://localhost:11434")

# -------------- Prompt helpers --------------
def _summarize_events_for_prompt(events: List[dict]) -> str:
    lines = []
    for e in events[:50]:
        ds = e.get("dataset") or "unknown"
        status = e.get("status") or "?"
        owner = e.get("owner") or "n/a"
        vio_parts = []
        for v in (e.get("violations") or []):
            dim = v.get("dimension") or "?"
            act = v.get("actual") if "actual" in v else v.get("actual_seconds")
            exp = v.get("expected")
            vio_parts.append(f"{dim}: actual={act} expected={exp}")
        vio_txt = "; ".join(vio_parts)
        errs = "; ".join(e.get("errors") or [])
        lines.append(f"- dataset={ds} status={status} owner={owner} violations=[{vio_txt}] errors=[{errs}]")
    return "\n".join(lines) or "(no events)"

# -------------- OpenAI backend --------------
_openai_client = None
def _get_openai_client():
    global _openai_client
    if _openai_client is None:
        from openai import OpenAI
        _openai_client = OpenAI()  # reads OPENAI_API_KEY from env
    return _openai_client

def _openai_answer(query: str, events: List[dict]) -> str:
    sys = (
        "You are GovernIQ, a data governance copilot. "
        "Be concise (max 6 sentences). Include dataset, owner, status, violations "
        "(freshness/completeness), and likely root cause. If nothing failed, say so."
    )
    user = f"Question: {query}\n\nRecent events:\n{_summarize_events_for_prompt(events)}\n\nAnswer clearly."
    try:
        client = _get_openai_client()
        res = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role":"system","content":sys},{"role":"user","content":user}],
            temperature=0.2,
            max_tokens=300,
        )
        return res.choices[0].message.content.strip()
    except Exception as ex:
        return f"(LLM openai unavailable – fallback) {ex}"

# -------------- Llama (Ollama) backend --------------
def _ollama_answer(query: str, events: List[dict]) -> str:
    import requests
    prompt = (
        "You are GovernIQ, a data governance copilot. "
        "Be concise (max 6 sentences). Include dataset, owner, status, violations "
        "(freshness/completeness), and likely root cause.\n\n"
        f"Question: {query}\n\nRecent events:\n{_summarize_events_for_prompt(events)}\n\nAnswer clearly."
    )
    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return (data.get("response") or "").strip() or "(empty response from Ollama)"
    except Exception as ex:
        return f"(LLM llama unavailable – fallback) {ex}"

# -------------- Rule-based fallback --------------
def _find_latest_for_dataset(events: List[dict], dataset: str):
    for e in events:
        if e.get("dataset","").lower() == dataset.lower():
            return e
    return None

def _rule_based_answer(q: str, events: List[dict]) -> str:
    q_low = q.lower()
    m = re.search(r"(dataset|table)\s+([a-zA-Z0-9_\-]+)", q_low)
    dataset = None
    if m: dataset = m.group(2)
    else:
        m2 = re.search(r"'([^']+)'|\"([^\"]+)\"", q)
        if m2: dataset = m2.group(1) or m2.group(2)

    if dataset:
        e = _find_latest_for_dataset(events, dataset)
        if not e:
            return f"I couldn't find any recent events for dataset '{dataset}'."
        status = e.get("status")
        owner = e.get("owner","unknown team")
        viols = e.get("violations") or []
        if status == "pass":
            return f"Latest check for dataset '{dataset}' passed. Owner: {owner}."
        parts = [f"Latest check for dataset '{dataset}' FAILED. Owner: {owner}."]
        for v in viols:
            dim = v.get("dimension","")
            if dim == "freshness":
                parts.append(f"- Freshness violated: actual age {v.get('actual_seconds','?')}s exceeds expected {v.get('expected')}.")
            elif dim == "completeness":
                parts.append(f"- Completeness below target: actual {v.get('actual','?')} vs expected {v.get('expected')}.")
        errs = e.get("errors") or []
        if errs: parts.append("Errors: " + "; ".join(errs[:3]))
        return " ".join(parts)

    fails = [e for e in events if e.get("status") == "fail"]
    if not fails: return "No recent failing events. All clear."
    by_ds = {}
    for e in fails:
        ds = e.get("dataset","unknown")
        by_ds[ds] = by_ds.get(ds, 0) + 1
    worst = sorted(by_ds.items(), key=lambda x: x[1], reverse=True)[:3]
    snippet = ", ".join([f"{ds} ({cnt} fails)" for ds, cnt in worst])
    return f"There are {len(fails)} recent failing events. Top offenders: {snippet}."

# -------------- Public entrypoint --------------
def answer_query(q: str, events: List[dict]) -> str:
    if not USE_LLM:
        return _rule_based_answer(q, events)
    if LLM_MODE == "llama":
        return _ollama_answer(q, events)
    # default: openai
    return _openai_answer(q, events)
