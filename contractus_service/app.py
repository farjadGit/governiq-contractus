from fastapi import FastAPI
from pydantic import BaseModel, Field
from typing import Dict, Any, List
import requests, os
from validator import validate_rows

from fastapi import Request
import urllib.parse

@app.post("/slack")
async def slack_handler(request: Request):
    form = await request.form()
    text = form.get("text", "")
    user = form.get("user_name", "")
    print(f"Slack command from {user}: {text}")

    # Use the existing LLM logic to answer
    events = store.fetch_events(limit=200)
    reply = answer_query(text, events)

    return {
        "response_type": "in_channel",
        "text": f"*GovernIQ*: {reply}"
    }


GOVERNIQ_URL = os.getenv("GOVERNIQ_URL", "http://localhost:8010")
app = FastAPI(title="Contractus Service", version="0.1.0")

class Contract(BaseModel):
    contract_id: str
    owner: str
    schema: Dict[str, str]
    sla: Dict[str, Any] = {}
    change_policy: Dict[str, Any] = {}

class ValidateRequest(BaseModel):
    contract: Contract
    rows: List[Dict[str, Any]] = Field(default_factory=list)


@app.post("/validate")
def validate(req: ValidateRequest):
    try:
        result = validate_rows(req.contract.dict(), req.rows)
    except Exception as ex:
        # Return error details for debugging
        return {"error": str(ex), "type": type(ex).__name__}
    event = {
        "dataset": req.contract.contract_id,
        "contract_id": req.contract.contract_id,
        "owner": req.contract.owner,
        "status": result["status"],
        "errors": result["errors"],
        "warnings": result["warnings"],
        "violations": result.get("violations", []),
    }
    try:
        r = requests.post(f"{GOVERNIQ_URL}/events", json=event, timeout=3)
        event["forwarded_to_governiq"] = r.status_code
    except Exception as ex:
        event["forwarded_to_governiq"] = f"failed: {ex}"
    return {"validation": result, "event": event}
