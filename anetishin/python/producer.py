import time
from confluent_kafka import Producer
from message import Event


class EventProducer:
    def __init__(self, brokers: str, topic: str):
        self.topic = topic
        self.counter = 0
        self.producer = Producer({
            "bootstrap.servers": brokers,
            # Идемпотентный продюсер: гарантирует ровно одну запись при повторах.
            # Требует acks=all; retries и max.in.flight автоматически выставляются библиотекой.
            "acks": "all",
            "enable.idempotence": True,
        })

    def run(self):
        while True:
            self.counter += 1
            event = Event.new(self.counter)
            try:
                # Синхронная отправка: flush после каждого сообщения
                self.producer.produce(self.topic, value=event.serialize())
                self.producer.flush()
                print(f"[producer] отправлено: {event}")
            except Exception as e:
                print(f"[producer] ошибка: {e}")
            time.sleep(0.5)
