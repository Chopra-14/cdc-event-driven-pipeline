from kafka import KafkaProducer
import json
import time

class KafkaEventProducer:
    def __init__(self, servers, topic):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5
        )

    def send(self, event):
        retry = 0
        while retry < 5:
            try:
                self.producer.send(self.topic, event)
                self.producer.flush()
                print("Event published:", event["operation_type"])
                return
            except Exception as e:
                retry += 1
                print("Kafka retry...", e)
                time.sleep(2 ** retry)
