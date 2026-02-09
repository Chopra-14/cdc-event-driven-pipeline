import uuid
from datetime import datetime
from decimal import Decimal

def normalize(data):
    if not data:
        return None

    clean = {}
    for k, v in data.items():
        if isinstance(v, Decimal):
            clean[k] = float(v)
        elif isinstance(v, datetime):
            clean[k] = v.isoformat()
        else:
            clean[k] = v
    return clean

def build_event(op, row_id, old, new):
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "table_name": "products",
        "operation_type": op,
        "primary_keys": {"id": row_id},
        "payload": {
            "old_data": normalize(old),
            "new_data": normalize(new)
        }
    }
