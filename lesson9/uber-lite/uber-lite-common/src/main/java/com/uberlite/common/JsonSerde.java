package com.uberlite.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

/**
 * Generic JSON serializer/deserializer for Kafka.
 * Uses Jackson instead of Kafka's JsonSerializer to avoid classpath issues.
 */
public class JsonSerde<T> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Returns a Serde for the given type (for use with Consumed.with). */
    public static <T> Serde<T> serde(Class<T> type) {
        return new Serde<T>() {
            @Override
            public Serializer<T> serializer() {
                return new JsonSerializer<>();
            }
            @Override
            public Deserializer<T> deserializer() {
                return new JsonDeserializer<>(type);
            }
        };
    }

    public static class JsonSerializer<T> implements Serializer<T> {
        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize: " + data, e);
            }
        }
    }

    public static class JsonDeserializer<T> implements Deserializer<T> {
        private final Class<T> type;

        public JsonDeserializer(Class<T> type) {
            this.type = type;
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            if (data == null) return null;
            try {
                return MAPPER.readValue(data, type);
            } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize", e);
            }
        }
    }
}
