package com.anetishin.kafka.consumer;

import com.anetishin.kafka.model.KafkaEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class SingleMessageConsumer {

    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper mapper;

    public SingleMessageConsumer(String brokers, String topic) {
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "single-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Автокоммит включён — коммитит после каждого poll
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // Читаем по одному сообщению за poll
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));
    }

    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        KafkaEvent event = mapper.readValue(record.value(), KafkaEvent.class);
                        System.out.printf("[single-consumer] получено (partition=%d, offset=%d): %s%n",
                                record.partition(), record.offset(), event);
                    } catch (Exception e) {
                        System.err.println("[single-consumer] ошибка десериализации: " + e.getMessage());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}
