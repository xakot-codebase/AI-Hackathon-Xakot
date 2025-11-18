import json, time, hashlib, os
from typing import Dict, Any, List

# In-memory store for serverless environments (Vercel, etc.)
_in_memory_events: Dict[str, List[Dict[str, Any]]] = {}


def _get_writable_path(original_path: str) -> str:
    """Get a writable path, using /tmp for serverless environments."""
    # Try original path first
    try:
        dir_path = os.path.dirname(original_path) or "."
        if dir_path and dir_path != ".":
            os.makedirs(dir_path, exist_ok=True)
        # Test write
        test_file = original_path + ".test"
        with open(test_file, "w") as f:
            f.write("test")
        os.remove(test_file)
        return original_path
    except (OSError, PermissionError):
        # Use /tmp for serverless environments (Vercel, AWS Lambda, etc.)
        filename = os.path.basename(original_path)
        tmp_path = os.path.join("/tmp", filename)
        return tmp_path


def append_event(audit_file: str, stage: str, payload: Dict[str, Any]) -> str:
    """Append event to audit log. Uses in-memory store if file writing fails."""
    event = {"stage": stage, "ts": int(time.time()), **payload}
    line = json.dumps(event, separators=(",", ":"))
    h = hashlib.sha256(line.encode()).hexdigest()
    event["hash"] = h
    
    # Try to write to file
    try:
        writable_path = _get_writable_path(audit_file)
        os.makedirs(os.path.dirname(writable_path) or ".", exist_ok=True)
        with open(writable_path, "a") as f:
            f.write(json.dumps(event) + "\n")
    except (OSError, PermissionError) as e:
        # Fallback to in-memory storage for serverless environments
        trace_id = payload.get("trace_id", "unknown")
        if trace_id not in _in_memory_events:
            _in_memory_events[trace_id] = []
        _in_memory_events[trace_id].append(event)
        # Silently continue - in-memory storage is sufficient for serverless
    
    return h


def read_trace(audit_file: str, trace_id: str) -> List[Dict[str, Any]]:
    """Read trace events. Checks both file and in-memory store."""
    events = []
    
    # Try to read from file
    try:
        writable_path = _get_writable_path(audit_file)
        if os.path.exists(writable_path):
            with open(writable_path) as f:
                for ln in f:
                    try:
                        ev = json.loads(ln)
                        if ev.get("trace_id") == trace_id:
                            events.append(ev)
                    except Exception:
                        continue
    except (OSError, PermissionError):
        pass
    
    # Also check in-memory store (for serverless environments)
    if trace_id in _in_memory_events:
        events.extend(_in_memory_events[trace_id])
    
    # Remove duplicates and sort
    seen_hashes = set()
    unique_events = []
    for ev in events:
        ev_hash = ev.get("hash")
        if ev_hash and ev_hash not in seen_hashes:
            seen_hashes.add(ev_hash)
            unique_events.append(ev)
        elif not ev_hash:
            unique_events.append(ev)
    
    unique_events.sort(key=lambda e: e.get("ts", 0))
    return unique_events
