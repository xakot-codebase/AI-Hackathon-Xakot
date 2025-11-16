from typing import Dict, Any
import os
import requests
import json

OPUS_BASE_URL = os.getenv("OPUS_BASE_URL", "https://operator.opus.com")
OPUS_WORKFLOW_ID = os.getenv("OPUS_WORKFLOW_ID", "")
OPUS_SERVICE_KEY = os.getenv("OPUS_SERVICE_KEY", "")

OPUS_API_URL = os.getenv("OPUS_API_URL", "")
OPUS_API_KEY = os.getenv("OPUS_API_KEY", "")


def _fallback_judgment(obj: Dict[str, Any],
                       rules: Dict[str, Any],
                       rule_result: Dict[str, Any]) -> Dict[str, Any]:
    violations = rule_result.get("violations", []) or []
    hard_violations = rule_result.get("hard_violations", []) or []

    if hard_violations:
        decision = "reject"
        conf = 0.95
        rationale = f"Hard violations present: {hard_violations}"
    elif violations:
        decision = "review"
        conf = 0.7
        rationale = f"Non-blocking violations present: {violations}"
    else:
        decision = "approve"
        conf = 0.98
        rationale = "No rule violations detected."

    return {"decision": decision, "confidence": conf, "rationale": rationale}


def ai_judgment(obj: Dict[str, Any],
                rules: Dict[str, Any],
                rule_result: Dict[str, Any]) -> Dict[str, Any]:
    api_url = None
    service_key = None
    
    if OPUS_API_URL:
        api_url = OPUS_API_URL
        service_key = OPUS_API_KEY or OPUS_SERVICE_KEY
    elif OPUS_WORKFLOW_ID and OPUS_SERVICE_KEY:
        api_url = f"{OPUS_BASE_URL}/api/v1/workflows/{OPUS_WORKFLOW_ID}/run"
        service_key = OPUS_SERVICE_KEY
    
    if not api_url or not service_key:
        return _fallback_judgment(obj, rules, rule_result)
    input_payload = {
        "instruction": (
            "You are the Xakot supply chain decision engine. "
            "Given a canonical document, the business rules, and which rules fired, "
            "you must decide whether to approve, reject, or send for human review. "
            "Return a JSON with fields: decision (approve|reject|review), "
            "rationale (string), confidence (0.0–1.0). "
            "Be strict on temperature excursions and safety issues."
        ),
        "canonical": obj,
        "rules": rules,
        "rule_result": rule_result,
    }

    try:
        resp = requests.post(
            api_url,
            headers={
                "x-service-key": service_key,
                "Content-Type": "application/json",
            },
            json={"input": input_payload},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()

        if all(k in data for k in ("decision", "rationale", "confidence")):
            return {
                "decision": data["decision"],
                "rationale": data["rationale"],
                "confidence": float(data["confidence"]),
            }

        if "output" in data and isinstance(data["output"], str):
            try:
                parsed = json.loads(data["output"])
                if all(k in parsed for k in ("decision", "rationale", "confidence")):
                    return {
                        "decision": parsed["decision"],
                        "rationale": parsed["rationale"],
                        "confidence": float(parsed["confidence"]),
                    }
            except json.JSONDecodeError:
                pass

    except Exception as e:
        print(f"[ai_judgment] Opus call failed: {e}")

    return _fallback_judgment(obj, rules, rule_result)