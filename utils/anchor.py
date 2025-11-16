import hashlib, json, time


def anchor_event_stub(payload) -> str:
    s = json.dumps(payload, sort_keys=True)
    h = hashlib.sha256(s.encode()).hexdigest()
    return f"0xstub{int(time.time())}{h[:16]}"
