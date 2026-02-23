package com.uberlite;

import org.apache.kafka.common.serialization.Deserializer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class DriverLocationDeserializer implements Deserializer<DriverLocationEvent> {
    @Override
    public DriverLocationEvent deserialize(String topic, byte[] data) {
        var buf = ByteBuffer.wrap(data);
        double lat = buf.getDouble();
        double lon = buf.getDouble();
        long ts = buf.getLong();
        byte[] statusBytes = new byte[buf.getInt()];
        buf.get(statusBytes);
        byte[] driverIdBytes = new byte[buf.getInt()];
        buf.get(driverIdBytes);
        return new DriverLocationEvent(
            new String(driverIdBytes, StandardCharsets.UTF_8),
            lat, lon, ts,
            new String(statusBytes, StandardCharsets.UTF_8)
        );
    }
}
