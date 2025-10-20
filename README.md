# Unified MVP: Contractus (Engine) + GovernIQ (AI Layer)

This MVP demonstrates an end-to-end flow:
1) Contractus validates data against a Data Contract (schema + SLAs).
2) It emits a structured event.
3) GovernIQ ingests events and answers natural-language questions about them.

## Quick Start (Local)

### Terminal 1 – GovernIQ
```bash
cd governiq_service
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --reload --port 8010
```

### Terminal 2 – Contractus
```bash
cd contractus_service
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export GOVERNIQ_URL="http://localhost:8010"  # Windows: set GOVERNIQ_URL=http://localhost:8010
uvicorn app:app --reload --port 8000
```

### PASS case
```bash
curl -X POST http://localhost:8000/validate -H "Content-Type: application/json" -d @- <<'JSON'
{
  "contract": {
  "contract_id": "sales_orders_v1",
  "owner": "team_sales",
  "schema": {
    "order_id": "string",
    "order_date": "datetime",
    "amount": "float",
    "currency": "string"
  },
  "sla": {
    "freshness": "1h",
    "freshness_field": "order_date",
    "completeness": ">=99%"
  },
  "change_policy": {
    "breaking_change_notice": "14d"
  }
},
  "rows": [
  {
    "order_id": "O1",
    "order_date": "2025-10-18T10:30:00",
    "amount": 12.5,
    "currency": "EUR"
  },
  {
    "order_id": "O2",
    "order_date": "2025-10-18T10:45:00",
    "amount": 9.9,
    "currency": "EUR"
  }
]
}
JSON
```

### FAIL (freshness) case
```bash
curl -X POST http://localhost:8000/validate -H "Content-Type: application/json" -d @- <<'JSON'
{
  "contract": {
  "contract_id": "sales_orders_v1",
  "owner": "team_sales",
  "schema": {
    "order_id": "string",
    "order_date": "datetime",
    "amount": "float",
    "currency": "string"
  },
  "sla": {
    "freshness": "1h",
    "freshness_field": "order_date",
    "completeness": ">=99%"
  },
  "change_policy": {
    "breaking_change_notice": "14d"
  }
},
  "rows": [
  {
    "order_id": "O1",
    "order_date": "2025-10-18T00:00:00",
    "amount": 12.5,
    "currency": "EUR"
  },
  {
    "order_id": "O2",
    "order_date": "2025-10-18T01:00:00",
    "amount": 9.9,
    "currency": "EUR"
  }
]
}
JSON
```

### Ask GovernIQ
```bash
curl "http://localhost:8010/ask?q=why%20did%20dataset%20sales_orders_v1%20fail%3F"
```
