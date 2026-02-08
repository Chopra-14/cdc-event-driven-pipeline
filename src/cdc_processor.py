import uuid
from datetime import datetime

def build_event(op, row_id, old, new):
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "table_name": "products",
        "operation_type": op,
        "primary_keys": {"id": row_id},
        "payload": {
            "old_data": old,
            "new_data": new
        }
    }
