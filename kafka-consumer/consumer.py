from kafka import KafkaConsumer
import json
import os

consumer = KafkaConsumer(
    os.getenv("KAFKA_TOPIC"),
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

for msg in consumer:
    print("EVENT:", msg.value)
