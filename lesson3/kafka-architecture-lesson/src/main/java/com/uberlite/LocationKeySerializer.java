package com.uberlite;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class LocationKeySerializer implements Serializer<LocationKey> {
    
    @Override
    public byte[] serialize(String topic, LocationKey key) {
        if (key == null) return null;
        
        byte[] entityIdBytes = key.entityId().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(4 + entityIdBytes.length + 8);
        
        buffer.putInt(entityIdBytes.length);
        buffer.put(entityIdBytes);
        buffer.putLong(key.h3Index());
        
        return buffer.array();
    }
}
