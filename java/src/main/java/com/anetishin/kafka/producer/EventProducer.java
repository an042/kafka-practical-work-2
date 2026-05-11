package com.anetishin.kafka.producer;

import com.anetishin.kafka.model.KafkaEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class EventProducer {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final ObjectMapper mapper;
    private final AtomicInteger counter = new AtomicInteger(0);

    public EventProducer(String brokers, String topic) {
        this.topic = topic;
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // At Least Once: acks=all + retries
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

        this.producer = new KafkaProducer<>(props);
    }

    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            int id = counter.incrementAndGet();
            KafkaEvent event = new KafkaEvent(id, "message-" + id, "producer", Instant.now());
            try {
                String json = mapper.writeValueAsString(event);
                // Синхронная отправка с ожиданием подтверждения
                producer.send(new ProducerRecord<>(topic, json)).get();
                System.out.println("[producer] отправлено: " + event);
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("[producer] ошибка: " + e.getMessage());
            }
        }
        producer.close();
    }
}
