import mysql.connector
import os
import time
import signal
from kafka_producer import KafkaEventProducer
from cdc_processor import build_event

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 5))

running = True

def shutdown(sig, frame):
    global running
    print("Shutting down CDC Service gracefully...")
    running = False

signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)

print("Starting CDC Service...")

conn = mysql.connector.connect(
    host=os.getenv("CDC_DB_HOST"),
    user=os.getenv("CDC_DB_USER"),
    password=os.getenv("CDC_DB_PASSWORD"),
    database=os.getenv("CDC_DB_NAME")
)

cursor = conn.cursor(dictionary=True)
producer = KafkaEventProducer(os.getenv("KAFKA_BOOTSTRAP_SERVERS"), os.getenv("KAFKA_TOPIC"))

previous = {}

while running:
    try:
        cursor.execute("SELECT * FROM products")
        rows = cursor.fetchall()

        current = {r["id"]: r for r in rows}

        for k, v in current.items():
            if k not in previous:
                producer.send(build_event("INSERT", k, None, v))
            elif previous[k] != v:
                producer.send(build_event("UPDATE", k, previous[k], v))

        for k in previous:
            if k not in current:
                producer.send(build_event("DELETE", k, previous[k], None))

        previous = current
        time.sleep(POLL_INTERVAL)

    except Exception as e:
        print("CDC Error:", e)
        time.sleep(5)

print("CDC Service stopped.")
