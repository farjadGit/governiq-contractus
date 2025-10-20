from typing import Dict, Any, List
import pandas as pd

TYPE_MAP = {
    "string": "string",
    "float": "float",
    "double": "float",
    "number": "float",
    "integer": "Int64",
    "int": "Int64",
    "boolean": "boolean",
    "bool": "boolean",
    "datetime": "datetime64[ns]",
    "timestamp": "datetime64[ns]",
    "date": "datetime64[ns]",
}

def _pandas_dtype_for(t: str) -> str:
    return TYPE_MAP.get((t or '').lower(), "string")

def validate_rows(contract: Dict[str, Any], rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    status = "pass"
    warnings, errors, violations = [], [], []
    schema = contract.get("schema") or {}
    sla = contract.get("sla") or {}
    if not schema:
        return {"status": "fail", "warnings": [], "errors": ["Missing 'schema' in contract"], "violations": []}

    df = pd.DataFrame(rows)

    for col, typ in schema.items():
        if col not in df.columns:
            errors.append(f"Missing required column '{col}'")
            continue
        pdt = _pandas_dtype_for(typ)
        try:
            if pdt == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=False)
                if df[col].isna().any():
                    errors.append(f"Column '{col}' has invalid datetime values")
            elif pdt == "boolean":
                df[col] = df[col].astype("boolean")
            else:
                df[col] = df[col].astype(pdt)
        except Exception as ex:
            errors.append(f"Column '{col}' failed coercion to {pdt}: {ex}")

    comp = (sla.get("completeness") or "").strip()
    if comp:
        try:
            target = float(comp.replace(">=", "").replace("%", "").strip())
            required_cols = list(schema.keys())
            ratio = (df[required_cols].notnull().all(axis=1).mean() * 100.0) if required_cols else 100.0
            if ratio + 1e-9 < target:
                errors.append(f"Completeness {ratio:.2f}% below target {target:.2f}%")
                violations.append({"dimension": "completeness", "actual": f"{ratio:.2f}%", "expected": comp})
        except Exception:
            warnings.append(f"Could not parse completeness SLA '{comp}'")

    freshness = (sla.get("freshness") or "").strip()
    freshness_field = (sla.get("freshness_field") or "").strip()
    if freshness and freshness_field and freshness_field in df.columns:
        import re
        m = re.match(r"(\d+)\s*([smhd])", freshness)
        if m:
            amount, unit = int(m.group(1)), m.group(2)
            seconds = {"s":1,"m":60,"h":3600,"d":86400}[unit] * amount
            ts = pd.to_datetime(df[freshness_field], errors="coerce")
            max_ts = ts.max()
            if pd.isna(max_ts):
                errors.append(f"No valid timestamps in '{freshness_field}' to assess freshness")
            else:
                now = pd.Timestamp.utcnow()
                # Ensure both are tz-naive
                max_ts_naive = max_ts.tz_localize(None) if max_ts.tzinfo is not None else max_ts
                now_naive = now.tz_localize(None) if now.tzinfo is not None else now
                age_sec = (now_naive - max_ts_naive).total_seconds()
                if age_sec > seconds:
                    errors.append(f"Freshness violated: latest '{freshness_field}' is older than {freshness}")
                    violations.append({"dimension": "freshness", "actual_seconds": int(age_sec), "expected": freshness})
        else:
            warnings.append(f"Could not parse freshness SLA '{freshness}'")

    if errors:
        status = "fail"
    return {"status": status, "warnings": warnings, "errors": errors, "violations": violations}
