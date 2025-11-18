# app.py — Fixed & improved Xakot FastAPI backend
from fastapi import FastAPI, Request, Body, HTTPException
from fastapi.responses import FileResponse
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
import time, uuid, os, asyncio, logging

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

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("xakot")

APP_DIR = os.path.dirname(__file__)
AUDIT_LOG = os.path.join(APP_DIR, "audit.jl")
RULES_FILE = os.path.join(APP_DIR, "rules.yaml")

app = FastAPI(
    title="Xakot × Opus AI — Intake → Enrich → Understand → Decide → Review → Deliver",
    version="2.0.0",
)


# ---------------------------
# Pydantic models (requests)
# ---------------------------

class IntakeDocument(BaseModel):
    doc_type: str = Field(..., example="invoice")
    file_url: Optional[str] = Field(None, example="samples/invoice.json")
    source: Optional[Dict[str, Any]] = Field(default_factory=dict)
    party: Optional[Dict[str, Any]] = None
    lines: Optional[List[Dict[str, Any]]] = None
    logistics: Optional[Dict[str, Any]] = None
    totals: Optional[Dict[str, Any]] = None
    policy: Optional[Dict[str, Any]] = None

    class Config:
        json_schema_extra = {
            "example": {
                "doc_type": "invoice",
                "file_url": "samples/invoice.json",
                "source": {"channel": "email", "filename": "inv_al_madina.json"},
                "party": {
                    "buyer": {"name": "Al Madina", "id": "BUY-001"},
                    "supplier": {"name": "Prime Meats", "id": "SUP-442"}
                },
                "lines": [
                    {"sku": "MEAT-RIBEYE-1KG", "desc": "Meat - ribeye", "qty": 25, "uom": "kg", "unit_price": 8.0}
                ],
                "totals": {"subtotal": 200.0, "tax": 0.0, "grand_total": 200.0, "currency": "USD"},
                "compliance": {"certs": ["halal"], "temperature_c": 6.2},
                "logistics": {
                    "delivery_id": "DEL-1001",
                    "from_coords": "55.2708,25.2048",
                    "to_coords": "55.3200,25.1200"
                }
            }
        }


# Removed IntakeBatchItem and IntakeBatchRequest - batch endpoint now accepts List[IntakeDocument] directly


class UnderstandRequest(BaseModel):
    trace_id: Optional[str] = None
    raw_payload: Optional[Dict[str, Any]] = None
    enrichment: Optional[Dict[str, Any]] = None
    # or canonical-like fields:
    source: Optional[Dict[str, Any]] = None
    doc_type: Optional[str] = None
    party: Optional[Dict[str, Any]] = None
    lines: Optional[List[Dict[str, Any]]] = None
    logistics: Optional[Dict[str, Any]] = None
    totals: Optional[Dict[str, Any]] = None


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
        extra = "allow"


class MetricsExportRequest(BaseModel):
    trace_id: Optional[str] = Field(default=None)
    artifact: Optional[Dict[str, Any]] = Field(default=None)
    sink: Optional[str] = Field(default="webhook")


# ---------------------------
# Helpers (unchanged logic)
# ---------------------------

def new_trace_id() -> str:
    return f"xakot-{int(time.time())}-{uuid.uuid4().hex[:6]}"


async def fetch_openfoodfacts_for_lines(lines: List[Dict[str, Any]]) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    async with httpx.AsyncClient(timeout=5.0) as client:
        tasks = []
        meta = []
        for idx, line in enumerate(lines or []):
            barcode = line.get("barcode") or line.get("ean") or line.get("ean13")
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
    new_lines = []
    for line in (lines or []):
        sku = line.get("sku")
        if sku:
            try:
                erp_info = mock_erp_price_service(sku)
                line["erp"] = erp_info
            except Exception as e:
                line["erp_error"] = str(e)
        new_lines.append(line)
    return new_lines


async def fetch_warehouse_temp_if_any(logistics: Dict[str, Any]) -> Dict[str, Any]:
    delivery_id = logistics.get("delivery_id") if logistics else None
    if not delivery_id:
        return {}
    try:
        return mock_warehouse_temperature(delivery_id)
    except Exception as e:
        return {"error": str(e)}


def build_canonical_from_raw(
    trace_id: str,
    raw_payload: Dict[str, Any],
    enrichment: Dict[str, Any],
) -> Canonical:
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
    return {"status": "queued", "sink": sink, "row": row}


def simple_agentic_review(canonical: Dict[str, Any],
                          verdict: Dict[str, Any],
                          rule_result: Dict[str, Any]) -> Dict[str, Any]:
    violations = rule_result.get("violations", [])
    status = "ok"
    override_decision = None
    if any(v.startswith("temperature_high") for v in violations):
        status = "needs_human_review"
        override_decision = "review"
    if any(v.startswith("missing_halal") for v in violations):
        status = "force_reject"
        override_decision = "reject"
    result = {"status": status, "original_decision": verdict.get("decision"), "violations": violations}
    if override_decision:
        result["override_decision"] = override_decision
    return result


# ---------------------------
# Root & Static endpoints
# ---------------------------

@app.get("/", response_model=Dict[str, Any])
def root():
    return {
        "message": "Xakot × Opus AI — Document Processing Pipeline",
        "version": "2.0.0",
        "endpoints": {"documentation": "/docs", "trace_viewer": "/web/trace_viewer.html", "api_base": "/"}
    }


@app.get("/web/trace_viewer.html")
def serve_viewer():
    path = os.path.join(APP_DIR, "web", "trace_viewer.html")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Trace viewer not found.")
    return FileResponse(path)


@app.get("/trace/{trace_id}", response_model=List[Dict[str, Any]])
def trace(trace_id: str):
    """
    Get raw event trace from audit log.
    
    Returns all events for a given trace_id in chronological order.
    Returns empty array if trace_id not found (for trace viewer compatibility).
    """
    try:
        events = read_trace(AUDIT_LOG, trace_id)
        # Return array directly for trace viewer compatibility
        return events if events else []
    except Exception as e:
        logger.exception("Error in /trace/{trace_id}")
        # Return empty array on error for trace viewer compatibility
        return []


# ---------------------------
# POST /intake
# ---------------------------

@app.post("/intake", response_model=Dict[str, Any])
async def intake(doc: IntakeDocument = Body(...)):
    """
    Intake a document and enrich it with external data sources.
    Accepts JSON body following IntakeDocument model.
    """
    try:
        payload = doc.model_dump()
        trace_id = new_trace_id()
        
        # Log intake started event
        append_event(AUDIT_LOG, "intake_started", {
            "trace_id": trace_id,
            "doc_type": payload.get("doc_type"),
            "file_url": payload.get("file_url"),
            "message": "Intake process started"
        })
        
        lines = payload.get("lines") or []
        logistics = payload.get("logistics") or {}

        # Log file processing if file_url exists
        if payload.get("file_url"):
            append_event(AUDIT_LOG, "file_received", {
                "trace_id": trace_id,
                "file_url": payload.get("file_url"),
                "message": f"Processing file: {payload.get('file_url')}"
            })

        # Parallel enrichment
        of_task = asyncio.create_task(fetch_openfoodfacts_for_lines(lines))
        osrm_task = asyncio.create_task(fetch_osrm_for_logistics(logistics))
        temp_task = asyncio.create_task(fetch_warehouse_temp_if_any(logistics))

        # ERP enrichment (sync)
        lines_enriched = enrich_lines_with_erp(lines)
        
        # Log ERP enrichment completed
        if lines_enriched:
            append_event(AUDIT_LOG, "erp_enrichment_completed", {
                "trace_id": trace_id,
                "lines_count": len(lines_enriched),
                "message": f"ERP enrichment completed for {len(lines_enriched)} lines"
            })

        openfoodfacts_result, osrm_result, temp_result = await asyncio.gather(of_task, osrm_task, temp_task)
        
        # Log external enrichment results
        if openfoodfacts_result:
            append_event(AUDIT_LOG, "openfoodfacts_enriched", {
                "trace_id": trace_id,
                "products_found": len(openfoodfacts_result),
                "message": f"OpenFoodFacts enrichment completed"
            })
        
        if osrm_result and not osrm_result.get("error"):
            append_event(AUDIT_LOG, "osrm_routing_completed", {
                "trace_id": trace_id,
                "message": "OSRM routing data retrieved"
            })
        
        if temp_result and not temp_result.get("error"):
            append_event(AUDIT_LOG, "temperature_retrieved", {
                "trace_id": trace_id,
                "message": "Warehouse temperature data retrieved"
            })

        enrichment = {
            "openfoodfacts": openfoodfacts_result,
            "osrm": osrm_result,
            "warehouse_temperature": temp_result,
            "lines_enriched": lines_enriched,
        }

        # Log intake completed
        event_payload = {"trace_id": trace_id, "raw_payload": payload, "enrichment": enrichment, "status": "intake"}
        event_hash = append_event(AUDIT_LOG, "intake_completed", event_payload)
        event_payload.setdefault("anchors", {})["audit_hash"] = event_hash

        logger.info("Intake completed: %s", trace_id)
        return event_payload

    except Exception as e:
        logger.exception("Error in /intake")
        # Log error event
        try:
            append_event(AUDIT_LOG, "intake_error", {
                "trace_id": trace_id if 'trace_id' in locals() else "unknown",
                "error": str(e),
                "message": "Intake process failed"
            })
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------
# POST /intake_batch
# ---------------------------

@app.post("/intake_batch", response_model=List[Dict[str, Any]])
async def intake_batch(docs: List[IntakeDocument] = Body(...)):
    """
    Intake multiple documents in batch and enrich each with external data sources.
    Accepts JSON array of IntakeDocument objects.
    Returns array of enriched documents with trace_ids.
    """
    results = []
    batch_trace_id = new_trace_id()
    
    try:
        # Log batch started event
        append_event(AUDIT_LOG, "batch_intake_started", {
            "batch_trace_id": batch_trace_id,
            "count": len(docs),
            "message": f"Batch intake process started for {len(docs)} documents"
        })
        
        for idx, doc in enumerate(docs):
            trace_id = None
            try:
                payload = doc.model_dump()
                trace_id = new_trace_id()
                
                # Log intake started event for each document
                append_event(AUDIT_LOG, "intake_started", {
                    "trace_id": trace_id,
                    "batch_trace_id": batch_trace_id,
                    "batch_index": idx,
                    "doc_type": payload.get("doc_type"),
                    "file_url": payload.get("file_url"),
                    "message": f"Intake process started for document {idx + 1}/{len(docs)}"
                })
                
                lines = payload.get("lines") or []
                logistics = payload.get("logistics") or {}

                # Log file processing if file_url exists
                if payload.get("file_url"):
                    append_event(AUDIT_LOG, "file_received", {
                        "trace_id": trace_id,
                        "file_url": payload.get("file_url"),
                        "message": f"Processing file: {payload.get('file_url')}"
                    })

                # Parallel enrichment
                of_task = asyncio.create_task(fetch_openfoodfacts_for_lines(lines))
                osrm_task = asyncio.create_task(fetch_osrm_for_logistics(logistics))
                temp_task = asyncio.create_task(fetch_warehouse_temp_if_any(logistics))

                # ERP enrichment (sync)
                lines_enriched = enrich_lines_with_erp(lines)
                
                # Log ERP enrichment completed
                if lines_enriched:
                    append_event(AUDIT_LOG, "erp_enrichment_completed", {
                        "trace_id": trace_id,
                        "lines_count": len(lines_enriched),
                        "message": f"ERP enrichment completed for {len(lines_enriched)} lines"
                    })

                openfoodfacts_result, osrm_result, temp_result = await asyncio.gather(of_task, osrm_task, temp_task)
                
                # Log external enrichment results
                if openfoodfacts_result:
                    append_event(AUDIT_LOG, "openfoodfacts_enriched", {
                        "trace_id": trace_id,
                        "products_found": len(openfoodfacts_result),
                        "message": f"OpenFoodFacts enrichment completed"
                    })
                
                if osrm_result and not osrm_result.get("error"):
                    append_event(AUDIT_LOG, "osrm_routing_completed", {
                        "trace_id": trace_id,
                        "message": "OSRM routing data retrieved"
                    })
                
                if temp_result and not temp_result.get("error"):
                    append_event(AUDIT_LOG, "temperature_retrieved", {
                        "trace_id": trace_id,
                        "message": "Warehouse temperature data retrieved"
                    })

                enrichment = {
                    "openfoodfacts": openfoodfacts_result,
                    "osrm": osrm_result,
                    "warehouse_temperature": temp_result,
                    "lines_enriched": lines_enriched,
                }

                # Log intake completed for this document
                event_payload = {
                    "trace_id": trace_id,
                    "batch_trace_id": batch_trace_id,
                    "batch_index": idx,
                    "raw_payload": payload,
                    "enrichment": enrichment,
                    "status": "intake"
                }
                event_hash = append_event(AUDIT_LOG, "intake_completed", event_payload)
                event_payload.setdefault("anchors", {})["audit_hash"] = event_hash
                results.append(event_payload)
                
            except Exception as item_error:
                logger.exception(f"Error processing document {idx} in batch")
                # Log error for this specific document but continue with others
                if trace_id is None:
                    trace_id = new_trace_id()
                try:
                    append_event(AUDIT_LOG, "intake_error", {
                        "trace_id": trace_id,
                        "batch_trace_id": batch_trace_id,
                        "batch_index": idx,
                        "error": str(item_error),
                        "message": f"Intake process failed for document {idx + 1}/{len(docs)}"
                    })
                    # Add error result to maintain array consistency
                    results.append({
                        "trace_id": trace_id,
                        "batch_trace_id": batch_trace_id,
                        "batch_index": idx,
                        "error": str(item_error),
                        "status": "error"
                    })
                except:
                    pass
        
        # Log batch completed event
        append_event(AUDIT_LOG, "batch_intake_completed", {
            "batch_trace_id": batch_trace_id,
            "total_count": len(docs),
            "successful_count": len([r for r in results if r.get("status") != "error"]),
            "failed_count": len([r for r in results if r.get("status") == "error"]),
            "message": f"Batch intake process completed"
        })
        
        logger.info("Batch intake completed: batch_trace_id=%s, processed=%d/%d", batch_trace_id, len(results), len(docs))
        return results
        
    except Exception as e:
        logger.exception("Error in /intake_batch")
        # Log batch error event
        try:
            append_event(AUDIT_LOG, "batch_intake_error", {
                "batch_trace_id": batch_trace_id if 'batch_trace_id' in locals() else "unknown",
                "error": str(e),
                "message": "Batch intake process failed"
            })
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------
# POST /understand
# ---------------------------

@app.post("/understand", response_model=Dict[str, Any])
async def understand(req: UnderstandRequest = Body(...)):
    try:
        body = req.model_dump()
        trace_id = body.get("trace_id") or new_trace_id()

        if body.get("raw_payload") and body.get("enrichment"):
            canonical = build_canonical_from_raw(trace_id=trace_id, raw_payload=body["raw_payload"], enrichment=body["enrichment"])
        else:
            canonical = Canonical(trace_id=trace_id, **{k: v for k, v in body.items() if k != "trace_id"})

        canonical.status = "understood"
        event_payload = {"trace_id": canonical.trace_id, "canonical": canonical.model_dump()}
        event_hash = append_event(AUDIT_LOG, "understand", event_payload)
        result = {**canonical.model_dump(), "anchors": {"audit_hash": event_hash}}
        return result
    except Exception as e:
        logger.exception("Error in /understand")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------
# POST /decide
# ---------------------------

@app.post("/decide", response_model=Dict[str, Any])
async def decide(canonical: Canonical = Body(...)):
    try:
        c_dict = canonical.model_dump()
        rules = load_rules(RULES_FILE)
        rule_result = apply_rules(c_dict, rules)

        verdict = ai_judgment(c_dict, rules, rule_result)
        confidence = verdict.get("confidence", 0.0)
        decision = verdict.get("decision", "review")

        if decision == "reject":
            status = "rejected"
        elif confidence >= 0.7 and not rule_result.get("hard_violations"):
            status = "decided"
        else:
            status = "agent_review"

        payload_for_trace = {"trace_id": c_dict["trace_id"], "canonical": c_dict, "status": status, "verdict": verdict, "rule_result": rule_result}
        event_hash = append_event(AUDIT_LOG, "decide", payload_for_trace)
        response_payload = {**c_dict, "status": status, "verdict": verdict, "rule_result": rule_result, "anchors": {"audit_hash": event_hash}}
        return response_payload
    except Exception as e:
        logger.exception("Error in /decide")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------
# POST /agent_review
# ---------------------------

@app.post("/agent_review", response_model=Dict[str, Any])
async def agent_review_endpoint(canonical: Canonical = Body(...)):
    try:
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

        event_hash = append_event(AUDIT_LOG, "agent_review", {"trace_id": c_dict["trace_id"], "agent_review": guardrail_result})
        c_dict.setdefault("anchors", {})["audit_hash"] = event_hash
        return c_dict
    except Exception as e:
        logger.exception("Error in /agent_review")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------
# POST /review
# ---------------------------

@app.post("/review", response_model=Dict[str, Any])
async def review(canonical: Canonical = Body(...), action: Optional[str] = None, note: Optional[str] = None):
    try:
        payload = canonical.model_dump()
        review_block = {"action": action or "approve", "note": note or "Looks good"}
        payload["review"] = review_block
        payload["status"] = "reviewed"
        event_hash = append_event(AUDIT_LOG, "review", {"trace_id": payload["trace_id"], "review": review_block})
        payload.setdefault("anchors", {})["audit_hash"] = event_hash
        return payload
    except Exception as e:
        logger.exception("Error in /review")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------
# POST /deliver
# ---------------------------

@app.post("/deliver", response_model=Dict[str, Any])
async def deliver(canonical: Canonical = Body(...)):
    try:
        payload = canonical.model_dump()
        payload["status"] = "delivered"
        chain_tx = anchor_event_stub(payload)
        payload.setdefault("anchors", {})["chain_tx"] = chain_tx
        verdict = payload.get("verdict") or {}
        decision = verdict.get("decision") if isinstance(verdict, dict) else None
        delivery_meta = {
            "doc_type": payload.get("doc_type"),
            "total_qty": payload.get("totals", {}).get("total_qty"),
            "decision": decision
        }
        payload["delivery_meta"] = delivery_meta
        event_hash = append_event(AUDIT_LOG, "deliver", {"trace_id": payload["trace_id"], "delivery_meta": delivery_meta})
        payload["anchors"]["audit_hash"] = event_hash
        return payload
    except Exception as e:
        logger.exception("Error in /deliver")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------
# GET /audit/{trace_id}/artifact
# ---------------------------

@app.get("/audit/{trace_id}/artifact", response_model=Dict[str, Any])
def audit_artifact(trace_id: str):
    try:
        events = read_trace(AUDIT_LOG, trace_id)
        artifact = build_artifact(trace_id, events)
        return {"trace_id": trace_id, "artifact": artifact}
    except Exception as e:
        logger.exception("Error in /audit/{trace_id}/artifact")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------
# POST /metrics/export
# ---------------------------

@app.post("/metrics/export", response_model=Dict[str, Any])
async def metrics_export(req: MetricsExportRequest = Body(...)):
    try:
        artifact: Optional[Dict[str, Any]] = req.artifact
        if not artifact and req.trace_id:
            events = read_trace(AUDIT_LOG, req.trace_id)
            artifact = build_artifact(req.trace_id, events)

        if not artifact:
            raise HTTPException(status_code=400, detail="Either trace_id or artifact must be provided.")

        row = metrics_row_from_artifact(artifact)
        send_result = await send_metrics_row(row, sink=req.sink or "webhook")
        return {"row": row, "send_result": send_result}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error in /metrics/export")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------
# Local dev runner
# ---------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)