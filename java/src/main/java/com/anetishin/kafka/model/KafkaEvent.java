package com.anetishin.kafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public class KafkaEvent {

    @JsonProperty("id")
    private int id;

    @JsonProperty("text")
    private String text;

    @JsonProperty("source")
    private String source;

    @JsonProperty("created_at")
    private Instant createdAt;

    public KafkaEvent() {}

    public KafkaEvent(int id, String text, String source, Instant createdAt) {
        this.id = id;
        this.text = text;
        this.source = source;
        this.createdAt = createdAt;
    }

    public int getId() { return id; }
    public String getText() { return text; }
    public String getSource() { return source; }
    public Instant getCreatedAt() { return createdAt; }

    @Override
    public String toString() {
        return "{id=" + id + ", text=" + text + ", source=" + source + ", createdAt=" + createdAt + "}";
    }
}
