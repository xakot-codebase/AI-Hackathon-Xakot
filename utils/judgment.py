from typing import Dict, Any
import os
import requests
import json

# Opus Platform API Configuration
# Base URL: https://operator.opus.com (from Opus documentation)
# Authentication: x-service-key header (not Authorization Bearer)
OPUS_BASE_URL = os.getenv("OPUS_BASE_URL", "https://operator.opus.com")
OPUS_WORKFLOW_ID = os.getenv("OPUS_WORKFLOW_ID", "")  # Get from workflow URL: app.opus.com/app/workflow/{workflow_id}
OPUS_SERVICE_KEY = os.getenv("OPUS_SERVICE_KEY", "")  # x-service-key from API Keys page

# Legacy support (for backward compatibility)
OPUS_API_URL = os.getenv("OPUS_API_URL", "")  # If set, will use this directly
OPUS_API_KEY = os.getenv("OPUS_API_KEY", "")  # Legacy key support


def _fallback_judgment(obj: Dict[str, Any],
                       rules: Dict[str, Any],
                       rule_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simple heuristic fallback if Opus is not available.
    You already have something like this – keep it as backup.
    """
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
    """
    Use Opus platform for AI judgment if configured.
    
    According to Opus documentation:
    - Base URL: https://operator.opus.com
    - Authentication: x-service-key header
    - Workflow ID required from workflow URL
    
    If not configured or call fails, fall back to heuristic judgment.
    """

    # Determine API endpoint and authentication
    api_url = None
    service_key = None
    
    # Priority 1: Use legacy OPUS_API_URL if set (backward compatibility)
    if OPUS_API_URL:
        api_url = OPUS_API_URL
        service_key = OPUS_API_KEY or OPUS_SERVICE_KEY
    # Priority 2: Use new Opus platform format with workflow ID
    elif OPUS_WORKFLOW_ID and OPUS_SERVICE_KEY:
        api_url = f"{OPUS_BASE_URL}/api/v1/workflows/{OPUS_WORKFLOW_ID}/run"
        service_key = OPUS_SERVICE_KEY
    
    # If no configuration, use fallback
    if not api_url or not service_key:
        return _fallback_judgment(obj, rules, rule_result)

    # Prepare input payload for Opus workflow
    # Adjust format based on Opus API documentation requirements
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
        # Opus API uses x-service-key header (not Authorization Bearer)
        resp = requests.post(
            api_url,
            headers={
                "x-service-key": service_key,  # Opus uses x-service-key header
                "Content-Type": "application/json",
            },
            json={"input": input_payload},  # Adjust payload format if needed
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()

        # --- Pattern 1: direct JSON fields present ---
        if all(k in data for k in ("decision", "rationale", "confidence")):
            return {
                "decision": data["decision"],
                "rationale": data["rationale"],
                "confidence": float(data["confidence"]),
            }

        # --- Pattern 2: result in ⁠ output ⁠ text – you parse JSON from it ---
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
                # fall-through to fallback
                pass

    except Exception as e:
        # In hackathon mode, don’t break the whole flow if Opus call fails;
        # log and fall back.
        print(f"[ai_judgment] Opus call failed: {e}")

    # Fallback if anything went wrong
    return _fallback_judgment(obj, rules, rule_result)