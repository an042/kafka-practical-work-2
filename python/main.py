import os
import threading

from producer import EventProducer
from consumer_single import SingleMessageConsumer
from consumer_batch import BatchMessageConsumer


def main():
    brokers = os.getenv("KAFKA_BROKERS", "kafka1:9092,kafka2:9092,kafka3:9092")
    topic   = os.getenv("KAFKA_TOPIC", "events")

    threads = [
        threading.Thread(target=EventProducer(brokers, topic).run, name="producer", daemon=True),
        threading.Thread(target=SingleMessageConsumer(brokers, topic).run, name="single-consumer", daemon=True),
        threading.Thread(target=BatchMessageConsumer(brokers, topic).run, name="batch-consumer", daemon=True),
    ]

    for t in threads:
        t.start()

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
