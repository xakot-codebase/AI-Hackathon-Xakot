from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
import time, uuid, os, asyncio

import httpx

from utils.audit import append_event, read_trace
from utils.rules import load_rules, apply_rules
from utils.judgment import ai_judgment
from utils.anchor import anchor_event_stub
from utils.legacy import (
    mock_erp_price_service,
    mock_warehouse_temperature,
)
from utils.artifacts import build_artifact

APP_DIR = os.path.dirname(__file__)
AUDIT_LOG = os.path.join(APP_DIR, "audit.jl")
RULES_FILE = os.path.join(APP_DIR, "rules.yaml")

app = FastAPI(
    title="Xakot × Opus AI — Intake → Enrich → Understand → Decide → Review → Deliver",
    version="2.0.0",
)


# -------------------------------------------------------------------
# Models
# -------------------------------------------------------------------

class Canonical(BaseModel):
    trace_id: str
    source: Dict[str, Any] = {}
    doc_type: str
    party: Dict[str, Any] = {}
    lines: List[Dict[str, Any]] = []
    logistics: Dict[str, Any] = {}
    totals: Dict[str, Any] = {}
    policy: Dict[str, Any] = {"min_stock_kg": 20, "auto_replenish": True}
    compliance: Dict[str, Any] = {}
    judgment_flags: List[str] = []
    status: str = "intake"
    anchors: Dict[str, Any] = {}

    class Config:
        extra = "allow"  # allow verdict, rule_result, artifacts, etc.


class IntakeBatchItem(BaseModel):
    payload: Dict[str, Any]


class IntakeBatchRequest(BaseModel):
    items: List[IntakeBatchItem]


class MetricsExportRequest(BaseModel):
    trace_id: Optional[str] = Field(
        default=None,
        description="If provided, metrics will be derived from this trace's artifact.",
    )
    artifact: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional artifact if already computed upstream.",
    )
    sink: Optional[str] = Field(
        default="webhook",
        description="Where to send metrics: 'email', 'sheet', 'webhook', etc.",
    )


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def new_trace_id() -> str:
    return f"xakot-{int(time.time())}-{uuid.uuid4().hex[:6]}"


async def fetch_openfoodfacts_for_lines(lines: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Demo: call OpenFoodFacts for any line that has 'barcode' or 'ean'.
    Returns a dict keyed by barcode.
    """
    results: Dict[str, Any] = {}
    async with httpx.AsyncClient(timeout=5.0) as client:
        tasks = []
        meta = []
        for idx, line in enumerate(lines):
            barcode = (
                line.get("barcode")
                or line.get("ean")
                or line.get("ean13")
            )
            if not barcode:
                continue
            url = f"https://world.openfoodfacts.org/api/v2/product/{barcode}.json"
            tasks.append(client.get(url))
            meta.append((idx, barcode))

        if not tasks:
            return results

        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for (idx, barcode), resp in zip(meta, responses):
            if isinstance(resp, Exception):
                results[barcode] = {"error": str(resp)}
                continue
            try:
                data = resp.json()
            except Exception as e:
                results[barcode] = {"error": str(e)}
                continue
            results[barcode] = data

    return results


async def fetch_osrm_for_logistics(logistics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Demo: call OSRM (or stub) for route metrics.
    Expects logistics to possibly contain 'from' and 'to' {lat, lon}.
    """
    origin = logistics.get("from")
    dest = logistics.get("to")
    if not origin or not dest:
        return {}

    base_url = "https://router.project-osrm.org/route/v1/driving"
    coord_str = f"{origin['lon']},{origin['lat']};{dest['lon']},{dest['lat']}"
    url = f"{base_url}/{coord_str}?overview=false"

    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return {"error": str(e)}


def enrich_lines_with_erp(lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    For each line with a 'sku', call mock_erp_price_service and attach ERP pricing.
    """
    new_lines = []
    for line in lines:
        sku = line.get("sku")
        if sku:
            erp_info = mock_erp_price_service(sku)
            line["erp"] = erp_info
        new_lines.append(line)
    return new_lines


async def fetch_warehouse_temp_if_any(logistics: Dict[str, Any]) -> Dict[str, Any]:
    delivery_id = logistics.get("delivery_id")
    if not delivery_id:
        return {}
    return mock_warehouse_temperature(delivery_id)


def build_canonical_from_raw(
    trace_id: str,
    raw_payload: Dict[str, Any],
    enrichment: Dict[str, Any],
) -> Canonical:
    """
    Build Canonical object from intake payload + enrichment.
    """
    return Canonical(
        trace_id=trace_id,
        source={
            "channel": raw_payload.get("channel", "upload"),
            "filename": raw_payload.get("filename", ""),
            "raw_source": raw_payload.get("source", {}),
        },
        doc_type=raw_payload.get("doc_type", "unknown"),
        party=raw_payload.get("party") or {},
        lines=enrichment.get("lines_enriched") or raw_payload.get("lines") or [],
        logistics=raw_payload.get("logistics") or {},
        totals=raw_payload.get("totals") or {},
        policy=raw_payload.get("policy") or {"min_stock_kg": 20, "auto_replenish": True},
        compliance=raw_payload.get("compliance") or {},
        judgment_flags=[],
        status="understood",
        anchors={"intake_enrichment": enrichment},
    )


def metrics_row_from_artifact(artifact: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compact metrics row for export (Sheets/BI/etc).
    """
    decision_block = artifact.get("decision") or {}
    extracted = artifact.get("extracted") or {}
    inputs = artifact.get("inputs") or {}
    doc_type = extracted.get("doc_type") or inputs.get("doc_type")

    lines = (extracted or {}).get("lines") or (inputs or {}).get("lines") or []
    total_qty = sum(l.get("qty", 0) for l in lines if isinstance(l.get("qty", 0), (int, float)))

    buyer = ((extracted or {}).get("party") or {}).get("buyer") or ((inputs or {}).get("party") or {}).get("buyer") or {}
    supplier = ((extracted or {}).get("party") or {}).get("supplier") or ((inputs or {}).get("party") or {}).get("supplier") or {}

    return {
        "trace_id": artifact.get("trace_id"),
        "doc_type": doc_type,
        "decision": decision_block.get("decision"),
        "confidence": decision_block.get("confidence"),
        "total_qty": total_qty,
        "buyer_name": buyer.get("name"),
        "supplier_name": supplier.get("name"),
    }


async def send_metrics_row(row: Dict[str, Any], sink: str = "webhook") -> Dict[str, Any]:
    """
    Stub for sending metrics to email / Sheet / webhook.
    For hackathon, we just return the row and pretend we queued it.
    """
    return {
        "status": "queued",
        "sink": sink,
        "row": row,
    }


def simple_agentic_review(canonical: Dict[str, Any],
                          verdict: Dict[str, Any],
                          rule_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simple policy guardrail:
    - If temperature_high => force human review.
    - If missing_halal_certificate => force reject.
    - Else keep AI decision.
    """
    violations = rule_result.get("violations", [])
    status = "ok"
    override_decision = None

    if any(v.startswith("temperature_high") for v in violations):
        status = "needs_human_review"
        override_decision = "review"
    if any(v.startswith("missing_halal") for v in violations):
        status = "force_reject"
        override_decision = "reject"

    result = {
        "status": status,
        "original_decision": verdict.get("decision"),
        "violations": violations,
    }
    if override_decision:
        result["override_decision"] = override_decision
    return result


# -------------------------------------------------------------------
# Root Endpoint
# -------------------------------------------------------------------

@app.get("/")
def root():
    """Root endpoint with API information."""
    return JSONResponse({
        "message": "Xakot × Opus AI — Document Processing Pipeline",
        "version": "2.0.0",
        "endpoints": {
            "documentation": "/docs",
            "trace_viewer": "/web/trace_viewer.html",
            "api_base": "/"
        }
    })


# -------------------------------------------------------------------
# UI & Trace
# -------------------------------------------------------------------

@app.get("/web/trace_viewer.html")
def serve_viewer():
    return FileResponse(os.path.join(APP_DIR, "web", "trace_viewer.html"))


@app.get("/trace/{trace_id}")
def trace(trace_id: str):
    return JSONResponse(read_trace(AUDIT_LOG, trace_id))


# -------------------------------------------------------------------
# POST /intake – single intake + parallel enrichment
# -------------------------------------------------------------------

@app.post("/intake")
async def intake(request: Request):
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        payload = await request.json()
    else:
        form = await request.form()
        payload = dict(form)

    trace_id = new_trace_id()
    lines = payload.get("lines") or []
    logistics = payload.get("logistics") or {}

    # Parallel enrichment:
    of_task = asyncio.create_task(fetch_openfoodfacts_for_lines(lines))
    osrm_task = asyncio.create_task(fetch_osrm_for_logistics(logistics))
    temp_task = asyncio.create_task(fetch_warehouse_temp_if_any(logistics))

    # ERP is sync; enrich lines directly
    lines_enriched = enrich_lines_with_erp(lines)

    openfoodfacts_result, osrm_result, temp_result = await asyncio.gather(
        of_task, osrm_task, temp_task
    )

    enrichment = {
        "openfoodfacts": openfoodfacts_result,
        "osrm": osrm_result,
        "warehouse_temperature": temp_result,
        "lines_enriched": lines_enriched,
    }

    event_payload = {
        "trace_id": trace_id,
        "raw_payload": payload,
        "enrichment": enrichment,
        "status": "intake",
    }

    event_hash = append_event(AUDIT_LOG, "intake", event_payload)
    event_payload.setdefault("anchors", {})["audit_hash"] = event_hash
    return JSONResponse(event_payload)


# -------------------------------------------------------------------
# POST /intake_batch – batch intake for multiple docs
# -------------------------------------------------------------------

@app.post("/intake_batch")
async def intake_batch(batch: IntakeBatchRequest):
    results = []
    for item in batch.items:
        payload = item.payload
        trace_id = new_trace_id()
        lines = payload.get("lines") or []
        logistics = payload.get("logistics") or {}

        of_task = asyncio.create_task(fetch_openfoodfacts_for_lines(lines))
        osrm_task = asyncio.create_task(fetch_osrm_for_logistics(logistics))
        temp_task = asyncio.create_task(fetch_warehouse_temp_if_any(logistics))
        lines_enriched = enrich_lines_with_erp(lines)

        openfoodfacts_result, osrm_result, temp_result = await asyncio.gather(
            of_task, osrm_task, temp_task
        )

        enrichment = {
            "openfoodfacts": openfoodfacts_result,
            "osrm": osrm_result,
            "warehouse_temperature": temp_result,
            "lines_enriched": lines_enriched,
        }

        event_payload = {
            "trace_id": trace_id,
            "raw_payload": payload,
            "enrichment": enrichment,
            "status": "intake",
        }
        event_hash = append_event(AUDIT_LOG, "intake", event_payload)
        event_payload.setdefault("anchors", {})["audit_hash"] = event_hash
        results.append(event_payload)

    return JSONResponse({"items": results})


# -------------------------------------------------------------------
# POST /understand – canonical structuring
# -------------------------------------------------------------------

@app.post("/understand")
async def understand(body: Dict[str, Any]):
    """
    Accept either:
    - Object returned by /intake (with raw_payload + enrichment + trace_id)
    - Or a canonical-like dict (then we wrap into Canonical)
    """
    trace_id = body.get("trace_id") or new_trace_id()

    if "raw_payload" in body and "enrichment" in body:
        canonical = build_canonical_from_raw(
            trace_id=trace_id,
            raw_payload=body["raw_payload"],
            enrichment=body["enrichment"],
        )
    else:
        canonical = Canonical(
            trace_id=trace_id,
            **{k: v for k, v in body.items() if k != "trace_id"},
        )

    canonical.status = "understood"

    # Important for build_artifact: store canonical under 'canonical'
    event_payload = {
        "trace_id": canonical.trace_id,
        "canonical": canonical.model_dump(),
    }

    event_hash = append_event(AUDIT_LOG, "understand", event_payload)
    result = {**canonical.model_dump(), "anchors": {"audit_hash": event_hash}}
    return JSONResponse(result)


# -------------------------------------------------------------------
# POST /decide – rules + AI judgment
# -------------------------------------------------------------------

@app.post("/decide")
async def decide(canonical: Canonical):
    c_dict = canonical.model_dump()
    rules = load_rules(RULES_FILE)
    rule_result = apply_rules(c_dict, rules)

    # Clean separation: rules first, then AI judgment
    verdict = ai_judgment(c_dict, rules, rule_result)

    confidence = verdict.get("confidence", 0.0)
    decision = verdict.get("decision", "review")

    if decision == "reject":
        status = "rejected"
    elif confidence >= 0.7 and not rule_result.get("hard_violations"):
        status = "decided"
    else:
        status = "agent_review"

    payload_for_trace = {
        "trace_id": c_dict["trace_id"],
        "canonical": c_dict,
        "status": status,
        "verdict": verdict,
        "rule_result": rule_result,
    }

    event_hash = append_event(AUDIT_LOG, "decide", payload_for_trace)
    response_payload = {
        **c_dict,
        "status": status,
        "verdict": verdict,
        "rule_result": rule_result,
        "anchors": {"audit_hash": event_hash},
    }
    return JSONResponse(response_payload)


# -------------------------------------------------------------------
# POST /agent_review – policy guardrail (agentic review)
# -------------------------------------------------------------------

@app.post("/agent_review")
async def agent_review_endpoint(canonical: Canonical):
    c_dict = canonical.model_dump()
    verdict = c_dict.get("verdict") or {}
    rule_result = c_dict.get("rule_result") or {}

    guardrail_result = simple_agentic_review(c_dict, verdict, rule_result)
    c_dict["agent_review"] = guardrail_result

    final_status = guardrail_result.get("status")
    if final_status == "force_reject":
        c_dict["status"] = "rejected"
    elif final_status == "needs_human_review":
        c_dict["status"] = "review"
    else:
        c_dict["status"] = c_dict.get("status", "decided")

    event_hash = append_event(
        AUDIT_LOG,
        "agent_review",
        {"trace_id": c_dict["trace_id"], "agent_review": guardrail_result},
    )
    c_dict.setdefault("anchors", {})["audit_hash"] = event_hash
    return JSONResponse(c_dict)


# -------------------------------------------------------------------
# POST /review – human review
# -------------------------------------------------------------------

@app.post("/review")
async def review(
    canonical: Canonical,
    action: Optional[str] = None,
    note: Optional[str] = None,
):
    payload = canonical.model_dump()
    review_block = {
        "action": action or "approve",
        "note": note or "Looks good",
    }
    payload["review"] = review_block
    payload["status"] = "reviewed"

    event_hash = append_event(
        AUDIT_LOG,
        "review",
        {"trace_id": payload["trace_id"], "review": review_block},
    )
    payload.setdefault("anchors", {})["audit_hash"] = event_hash
    return JSONResponse(payload)


# -------------------------------------------------------------------
# POST /deliver – final delivery + anchor
# -------------------------------------------------------------------

@app.post("/deliver")
async def deliver(canonical: Canonical):
    payload = canonical.model_dump()
    payload["status"] = "delivered"
    chain_tx = anchor_event_stub(payload)
    payload.setdefault("anchors", {})["chain_tx"] = chain_tx

    delivery_meta = {
        "doc_type": payload.get("doc_type"),
        "total_qty": payload.get("totals", {}).get("total_qty"),
        "decision": payload.get("verdict", {}).get("decision"),
    }
    payload["delivery_meta"] = delivery_meta

    event_hash = append_event(
        AUDIT_LOG,
        "deliver",
        {"trace_id": payload["trace_id"], "delivery_meta": delivery_meta},
    )
    payload["anchors"]["audit_hash"] = event_hash
    return JSONResponse(payload)


# -------------------------------------------------------------------
# GET /audit/{trace_id}/artifact – full decision artifact
# -------------------------------------------------------------------

@app.get("/audit/{trace_id}/artifact")
def audit_artifact(trace_id: str):
    events = read_trace(AUDIT_LOG, trace_id)
    artifact = build_artifact(trace_id, events)
    return JSONResponse({"trace_id": trace_id, "artifact": artifact})


# -------------------------------------------------------------------
# POST /metrics/export – export compact metrics row
# -------------------------------------------------------------------

@app.post("/metrics/export")
async def metrics_export(req: MetricsExportRequest):
    artifact: Optional[Dict[str, Any]] = req.artifact
    if not artifact and req.trace_id:
        events = read_trace(AUDIT_LOG, req.trace_id)
        artifact = build_artifact(req.trace_id, events)

    if not artifact:
        return JSONResponse(
            {"error": "Either trace_id or artifact must be provided."},
            status_code=400,
        )

    row = metrics_row_from_artifact(artifact)
    send_result = await send_metrics_row(row, sink=req.sink or "webhook")
    return JSONResponse({"row": row, "send_result": send_result})


# -------------------------------------------------------------------
# Local Dev Entry Point
# -------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
