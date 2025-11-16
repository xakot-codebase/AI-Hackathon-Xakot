import json, time, hashlib, os
from typing import Dict, Any, List


def append_event(audit_file: str, stage: str, payload: Dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(audit_file), exist_ok=True)
    event = {"stage": stage, "ts": int(time.time()), **payload}
    line = json.dumps(event, separators=(",", ":"))
    h = hashlib.sha256(line.encode()).hexdigest()
    event["hash"] = h
    with open(audit_file, "a") as f:
        f.write(json.dumps(event) + "\n")
    return h


def read_trace(audit_file: str, trace_id: str) -> List[Dict[str, Any]]:
    events = []
    if not os.path.exists(audit_file):
        return events
    with open(audit_file) as f:
        for ln in f:
            try:
                ev = json.loads(ln)
                if ev.get("trace_id") == trace_id:
                    events.append(ev)
            except Exception:
                continue
    events.sort(key=lambda e: e.get("ts", 0))
    return events
