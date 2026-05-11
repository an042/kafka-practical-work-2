from confluent_kafka import Consumer, TopicPartition
from message import Event

BATCH_SIZE = 10


class BatchMessageConsumer:
    def __init__(self, brokers: str, topic: str):
        self.consumer = Consumer({
            "bootstrap.servers": brokers,
            "group.id": "batch-consumer-group",
            # Ручной коммит
            "enable.auto.commit": False,
            # fetch.min.bytes — брокер ждёт накопления данных
            "fetch.min.bytes": 1024,
            # fetch.max.wait.ms — максимальное ожидание данных от брокера
            "fetch.max.wait.ms": 500,
            "auto.offset.reset": "earliest",
        })
        self.consumer.subscribe([topic])

    def run(self):
        batch = []
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"[batch-consumer] ошибка: {msg.error()}")
                    continue
                batch.append(msg)
                if len(batch) >= BATCH_SIZE:
                    self._process_batch(batch)
                    batch.clear()
        finally:
            self.consumer.close()

    def _process_batch(self, batch):
        offsets = []
        for msg in batch:
            try:
                event = Event.deserialize(msg.value())
                print(f"[batch-consumer] обработано (partition={msg.partition()}, offset={msg.offset()}): {event}")
            except Exception as e:
                print(f"[batch-consumer] ошибка десериализации: {e}")
            offsets.append(TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1))
        # Один коммит для всей пачки
        self.consumer.commit(offsets=offsets)
        print(f"[batch-consumer] закоммичена пачка из {len(batch)} сообщений")
