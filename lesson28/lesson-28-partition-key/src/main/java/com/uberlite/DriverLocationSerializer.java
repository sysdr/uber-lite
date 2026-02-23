package com.uberlite;

import org.apache.kafka.common.serialization.Serializer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Wire format: [lat(8)][lon(8)][timestampMs(8)][statusLen(4)][status(N)][driverIdLen(4)][driverId(M)]
 * Partitioner reads only bytes 0-15 (lat+lon) — no full deserialization needed at routing time.
 */
public class DriverLocationSerializer implements Serializer<DriverLocationEvent> {
    @Override
    public byte[] serialize(String topic, DriverLocationEvent event) {
        byte[] statusBytes = event.status().getBytes(StandardCharsets.UTF_8);
        byte[] driverIdBytes = event.driverId().getBytes(StandardCharsets.UTF_8);
        int capacity = 8 + 8 + 8 + 4 + statusBytes.length + 4 + driverIdBytes.length;
        return ByteBuffer.allocate(capacity)
            .putDouble(event.lat())
            .putDouble(event.lon())
            .putLong(event.timestampMs())
            .putInt(statusBytes.length)
            .put(statusBytes)
            .putInt(driverIdBytes.length)
            .put(driverIdBytes)
            .array();
    }
}
