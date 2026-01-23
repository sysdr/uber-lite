package com.uberlite.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.*;
import com.uberlite.model.DriverLocation;

public class JsonSerdes {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    public static Serde<DriverLocation> driverLocation() {
        return Serdes.serdeFrom(
            new JsonSerializer<>(),
            new JsonDeserializer<>(DriverLocation.class)
        );
    }
    
    static class JsonSerializer<T> implements Serializer<T> {
        @Override
        public byte[] serialize(String topic, T data) {
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    static class JsonDeserializer<T> implements Deserializer<T> {
        private final Class<T> type;
        
        JsonDeserializer(Class<T> type) {
            this.type = type;
        }
        
        @Override
        public T deserialize(String topic, byte[] data) {
            try {
                return MAPPER.readValue(data, type);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
