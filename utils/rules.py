import yaml
from typing import Dict, Any
from pydantic import BaseModel


def load_rules(path: str) -> Dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f)


def apply_rules(obj: Dict[str, Any], rules: Dict[str, Any]) -> Dict[str, Any]:
    violations = []

    # Inventory threshold
    min_kg = rules.get("stock_rules", {}).get("auto_replenish_if_below_kg", 20)
    qty_kg = 0
    for ln in obj.get("lines", []):
        if str(ln.get("uom", "")).lower() == "kg":
            qty_kg += float(ln.get("qty", 0))
    if qty_kg < min_kg:
        violations.append(f"inventory_below_threshold:{qty_kg}kg<{min_kg}kg")

    # Allowed SKUs
    allowed = set(rules.get("stock_rules", {}).get("allowed_skus", []))
    for ln in obj.get("lines", []):
        sku = ln.get("sku")
        if allowed and sku not in allowed:
            violations.append(f"sku_not_allowed:{sku}")

    # Currency allowlist
    currency = (obj.get("totals") or {}).get("currency") or "USD"
    if currency not in rules.get("invoice_rules", {}).get(
        "currency_allowlist", ['USD', 'AED', 'ZAR', 'INR']
    ):
        violations.append(f"currency_not_allowed:{currency}")

    # Meat category → halal certificate required
    need_halal = "meat" in ",".join(
        [ln.get("desc", "").lower() for ln in obj.get("lines", [])]
    )
    if need_halal:
        certs = (obj.get("compliance") or {}).get("certs", [])
        if "halal" not in [c.lower() for c in certs]:
            violations.append("missing_halal_certificate")

    # Temperature check
    temp = (obj.get("compliance") or {}).get("temperature_c")
    if temp is not None and float(temp) > 5.0:
        violations.append(f"temperature_high:{temp}C")

    return {"violations": violations, "qty_kg": qty_kg, "currency": currency}
