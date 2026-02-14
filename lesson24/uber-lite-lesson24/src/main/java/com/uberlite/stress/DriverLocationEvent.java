package com.uberlite.stress;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Immutable driver location event (Java 21 Record).
 * Serializes to compact JSON for Kafka producer.
 */
public record DriverLocationEvent(
        String driverId,
        double latitude,
        double longitude,
        String h3Cell,      // H3 index string at Resolution 9
        int    h3Resolution,
        long   timestampMs
) {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public byte[] toJsonBytes() {
        try {
            return MAPPER.writeValueAsBytes(this);
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    public static DriverLocationEvent fromJsonBytes(byte[] bytes) {
        try {
            return MAPPER.readValue(bytes, DriverLocationEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Deserialization failed", e);
        }
    }
}
