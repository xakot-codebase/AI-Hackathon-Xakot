
# XAKOT – Autonomous Supply Chain Intelligence Platform

Xakot is an AI-driven, trust-adaptive platform that automates supply-chain validation by combining  
**document ingestion, IoT telemetry, ERP enrichment, rules engines, AI judgment, agentic review,  
human approval, blockchain auditability, and exportable metrics** into a unified workflow.

---

## ⚡ Features

### **1. Multi-Source Intake**
- JSON, PDF (via OCR), email, uploads  
- Supplier invoices, POs, GRNs, QC forms  
- Sample batch ingestion included  

### **2. Parallel Enrichment**
- ERP product pricing (legacy mock API)  
- Temperature telemetry  
- OSRM routing  
- OpenFoodFacts product metadata  
- All async via `asyncio.gather()`  

### **3. Canonicalization**
- Converts raw incoming data into a unified structure  
- Normalizes fields for downstream processing  

### **4. Rules + AI Judgment + Agentic Review**
- Deterministic rules (`rules.yaml`)  
- AI layer giving decision, rationale & confidence  
- Policy guardrails enforcing compliance boundaries  
- Human review only when needed  

### **5. Delivery + Audit Trail**
- Anchored decision outputs  
- Blockchain-ready audit hashes  
- Full trace viewer UI included (`/web/trace_viewer.html`)  

### **6. Decision Artifacts + Metrics Export**
- Creates a compact metrics row for BI systems  
- Full decision artifact endpoint  
- Ideal for dashboards and analytics pipelines  

---

## ▣ Project Structure

```
xakot_opus_demo_v2/
  app.py
  requirements.txt
  rules.yaml
  audit.jl                 # runtime generated
  utils/
    audit.py
    rules.py
    judgment.py
    anchor.py
    artifact.py
    legacy.py
  web/
    trace_viewer.html
  samples/
    purchase_order.json
    invoice.json
    batch_invoices.json
```

---

## ◉ API Endpoints

### **POST /intake**
Multi-source document ingestion + enrichment.

### **POST /intake_batch**
Submit multiple documents for batch processing.

### **POST /understand**
Canonical transform.

### **POST /decide**
Runs rules and AI judgment.

### **POST /agent_review**
Policy enforcement layer.

### **POST /review**
Human review and approval.

### **POST /deliver**
Final step with audit anchoring.

### **GET /audit/{trace_id}/artifact**
Returns full decision artifact.

### **POST /metrics/export**
Returns a compact, BI-ready metrics row.

### **GET /trace/{trace_id}**
Returns raw event trace from audit log.

### **GET /web/trace_viewer.html**
UI for exploring decisions.

---

## ▶ Running the Project

### **Install dependencies**
```
pip install -r requirements.txt
```

### **Run FastAPI app**
```
uvicorn app:app --reload
```

### **Access UI**
```
http://localhost:8000/web/trace_viewer.html
```

---

## ◈ Sample Commands

### **Intake a document**
```
POST /intake
{
  "doc_type": "invoice",
  "source": {"channel": "email"},
  "party": {...},
  "lines": [...],
  "totals": {...}
}
```

### **Intake a batch**
```
POST /intake_batch
[
  {... invoice 1 ...},
  {... invoice 2 ...}
]
```

---

## ◆ Tech Stack
- **FastAPI**  
- **asyncio** for parallel enrichment  
- **Pydantic** for canonical modeling  
- **YAML rules engine**  
- **LLM judgment layer (stubbed or pluggable)**  
- **SHA256 audit hashing**  
- **HTML-based trace viewer**  

---

## ▸ Hackathon Summary
Xakot demonstrates:
- Multi-source AI-driven enrichment  
- Autonomous validation pipeline  
- Agentic governance  
- Auditability & compliance  
- Production-grade structure  
- Strong real-world use cases (cold-chain, food safety, distribution)

This aligns directly with hackathon requirements for:
- Agentic workflows  
- Decision traceability  
- Document + API + IoT integration  
- Autonomous pipelines  
- Human-in-the-loop design  

---

## ▪ Documentation
See `Xakot_ns_Convoy_Investor_Summary` for full product, business, and technical strategy.

---

## ● Contact
Built for hackathon demonstration by Xakot Developer Team.

