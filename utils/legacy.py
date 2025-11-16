from typing import Dict, Any

def mock_erp_price_service(sku: str) -> Dict[str, Any]:
    mock_db = {
        "MEAT-RIBEYE-1KG": {"sku": "MEAT-RIBEYE-1KG", "erp_price": 7.5, "currency": "USD"},
        "MEAT-TBONE-1KG":  {"sku": "MEAT-TBONE-1KG",  "erp_price": 7.0, "currency": "USD"},
    }
    return mock_db.get(sku, {"sku": sku, "erp_price": None, "currency": "USD"})

def mock_erp_products_page(page: int = 1, page_size: int = 2) -> Dict[str, Any]:
    all_products = [
        {"sku": "MEAT-RIBEYE-1KG", "name": "Ribeye", "default_uom": "kg"},
        {"sku": "MEAT-TBONE-1KG",  "name": "T-Bone", "default_uom": "kg"},
        {"sku": "MEAT-MINCE-1KG",  "name": "Mince", "default_uom": "kg"},
        {"sku": "MEAT-STEAK-1KG",  "name": "Steak", "default_uom": "kg"},
    ]
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "page": page,
        "page_size": page_size,
        "total": len(all_products),
        "items": all_products[start:end],
        "has_next": end < len(all_products),
    }

def mock_warehouse_temperature(delivery_id: str) -> Dict[str, Any]:
    return {
        "delivery_id": delivery_id,
        "avg_temp_c": 6.2,
        "peak_temp_c": 7.0,
        "sensor_source": "mock_warehouse_temp_api",
    }
