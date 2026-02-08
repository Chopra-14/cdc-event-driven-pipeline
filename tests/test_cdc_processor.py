from src.cdc_processor import build_event

def test_event():
    event = build_event("INSERT",1,None,{"a":1})
    assert event["operation_type"]=="INSERT"
