from confluent_kafka import Consumer
from message import Event


class SingleMessageConsumer:
    def __init__(self, brokers: str, topic: str):
        self.consumer = Consumer({
            "bootstrap.servers": brokers,
            "group.id": "single-consumer-group",
            # Автокоммит включён
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 1000,
            "auto.offset.reset": "earliest",
        })
        self.consumer.subscribe([topic])

    def run(self):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"[single-consumer] ошибка: {msg.error()}")
                    continue
                try:
                    event = Event.deserialize(msg.value())
                    print(f"[single-consumer] получено (partition={msg.partition()}, offset={msg.offset()}): {event}")
                except Exception as e:
                    print(f"[single-consumer] ошибка десериализации: {e}")
        finally:
            self.consumer.close()
