package com.anetishin.kafka;

import com.anetishin.kafka.consumer.BatchMessageConsumer;
import com.anetishin.kafka.consumer.SingleMessageConsumer;
import com.anetishin.kafka.producer.EventProducer;

public class App {

    public static void main(String[] args) throws InterruptedException {
        String brokers = System.getenv().getOrDefault("KAFKA_BROKERS", "kafka1:9092,kafka2:9092,kafka3:9092");
        String topic   = System.getenv().getOrDefault("KAFKA_TOPIC", "events");

        Thread producerThread = new Thread(new EventProducer(brokers, topic)::run, "producer");
        Thread singleThread   = new Thread(new SingleMessageConsumer(brokers, topic)::run, "single-consumer");
        Thread batchThread    = new Thread(new BatchMessageConsumer(brokers, topic)::run, "batch-consumer");

        producerThread.start();
        singleThread.start();
        batchThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producerThread.interrupt();
            singleThread.interrupt();
            batchThread.interrupt();
        }));

        producerThread.join();
        singleThread.join();
        batchThread.join();
    }
}
