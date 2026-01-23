package com.uberlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class DriverLocationSerializer implements Serializer<DriverLocation> {
    private final ObjectMapper mapper = new ObjectMapper();
    
    @Override
    public byte[] serialize(String topic, DriverLocation data) {
        if (data == null) return null;
        try {
            return mapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize DriverLocation", e);
        }
    }
}
