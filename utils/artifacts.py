from typing import List, Dict, Any


def build_artifact(trace_id: str,
                   events: List[Dict[str, Any]]) -> Dict[str, Any]:
    def first(stage: str) -> Dict[str, Any]:
        return next((e for e in events if e.get("stage") == stage), {})

    decide_ev = first("decide")

    return {
        "trace_id": trace_id,
        "timestamps": [e.get("ts") for e in events],
        "inputs": first("intake").get("raw_payload"),
        "extracted": first("understand").get("canonical"),
        "decision": decide_ev.get("verdict"),
        "rules_fired": decide_ev.get("rule_result"),
        "agentic_review": first("agent_review").get("agent_review"),
        "human_review": first("review").get("review"),
        "delivery": first("deliver").get("delivery_meta"),
        "external_sources": [
            e.get("origin_url") for e in events if "origin_url" in e
        ],
    }
