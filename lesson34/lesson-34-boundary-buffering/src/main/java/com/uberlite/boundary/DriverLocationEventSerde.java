package com.uberlite.boundary;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * JSON Serde for DriverLocationEvent.
 * Production would use Avro + Schema Registry; JSON is used here for debuggability.
 */
public class DriverLocationEventSerde implements Serde<DriverLocationEvent> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Serializer<DriverLocationEvent> serializer() {
        return (topic, data) -> {
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Serialization failed", e);
            }
        };
    }

    @Override
    public Deserializer<DriverLocationEvent> deserializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.readValue(data, DriverLocationEvent.class);
            } catch (Exception e) {
                throw new RuntimeException("Deserialization failed", e);
            }
        };
    }
}
