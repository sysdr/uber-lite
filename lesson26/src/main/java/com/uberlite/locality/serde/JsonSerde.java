package com.uberlite.locality.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

/**
 * Generic JSON Serde backed by Jackson.
 * No schema registry dependency — this lesson is about partition routing,
 * not schema evolution. Production would use Avro + Confluent SR.
 */
public final class JsonSerde<T> implements Serde<T> {

    private static final ObjectMapper MAPPER = JsonMapper.builder().build();
    private final Class<T> type;

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    @Override
    public Serializer<T> serializer() {
        return new Serializer<>() {
            @Override public void configure(Map<String, ?> configs, boolean isKey) {}
            @Override
            public byte[] serialize(String topic, T data) {
                if (data == null) return null;
                try {
                    return MAPPER.writeValueAsBytes(data);
                } catch (IOException e) {
                    throw new RuntimeException("JSON serialization failed for " + type.getSimpleName(), e);
                }
            }
            @Override public void close() {}
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return new Deserializer<>() {
            @Override public void configure(Map<String, ?> configs, boolean isKey) {}
            @Override
            public T deserialize(String topic, byte[] data) {
                if (data == null) return null;
                try {
                    return MAPPER.readValue(data, type);
                } catch (IOException e) {
                    throw new RuntimeException("JSON deserialization failed for " + type.getSimpleName(), e);
                }
            }
            @Override public void close() {}
        };
    }

    @Override public void configure(Map<String, ?> configs, boolean isKey) {}
    @Override public void close() {}
}
