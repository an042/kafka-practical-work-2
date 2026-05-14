package com.anetishin.kafka.consumer;

import com.anetishin.kafka.model.KafkaEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class BatchMessageConsumer {

    private static final int BATCH_SIZE = 10;

    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper mapper;

    public BatchMessageConsumer(String brokers, String topic) {
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "batch-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Ручной коммит
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // fetch.min.bytes — брокер ждёт накопления данных
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1024");
        // fetch.max.wait.ms — максимальное ожидание
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));
    }

    public void run() {
        List<ConsumerRecord<String, String>> batch = new ArrayList<>();
        try {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    batch.add(record);
                    if (batch.size() >= BATCH_SIZE) {
                        processBatch(batch);
                        batch.clear();
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    private void processBatch(List<ConsumerRecord<String, String>> batch) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (ConsumerRecord<String, String> record : batch) {
            try {
                KafkaEvent event = mapper.readValue(record.value(), KafkaEvent.class);
                System.out.printf("[batch-consumer] обработано (partition=%d, offset=%d): %s%n",
                        record.partition(), record.offset(), event);
                // Оффсет добавляем только при успешной обработке
                offsets.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)
                );
            } catch (Exception e) {
                System.err.println("[batch-consumer] ошибка десериализации: " + e.getMessage());
            }
        }
        // Один синхронный коммит для успешно обработанных сообщений
        if (!offsets.isEmpty()) {
            consumer.commitSync(offsets);
            System.out.println("[batch-consumer] закоммичена пачка из " + offsets.size() + " сообщений");
        }
    }
}
