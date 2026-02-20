package com.uberlite.lesson27;

import com.uberlite.lesson27.Models.*;
import org.apache.kafka.common.serialization.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Manual ByteBuffer serialization.
 * No Jackson, no Avro, no schema registry — we control every byte.
 * DriverLocation wire format (fixed 44 bytes):
 *   [driverId-length: 4][driverId-bytes: N][lat: 8][lng: 8][h3Cell: 8][ts: 8]
 */
public final class Serialization {

    // ── DriverLocation ────────────────────────────────────────────────────────

    public static class DriverLocationSerializer implements Serializer<DriverLocation> {
        @Override
        public byte[] serialize(String topic, DriverLocation data) {
            if (data == null) return null;
            byte[] idBytes = data.driverId().getBytes(StandardCharsets.UTF_8);
            var buf = ByteBuffer.allocate(4 + idBytes.length + 8 + 8 + 8 + 8);
            buf.putInt(idBytes.length);
            buf.put(idBytes);
            buf.putDouble(data.lat());
            buf.putDouble(data.lng());
            buf.putLong(data.h3Cell());
            buf.putLong(data.timestampMs());
            return buf.array();
        }
    }

    public static class DriverLocationDeserializer implements Deserializer<DriverLocation> {
        @Override
        public DriverLocation deserialize(String topic, byte[] data) {
            if (data == null) return null;
            var buf = ByteBuffer.wrap(data);
            int idLen   = buf.getInt();
            byte[] idB  = new byte[idLen];
            buf.get(idB);
            return new DriverLocation(
                new String(idB, StandardCharsets.UTF_8),
                buf.getDouble(),
                buf.getDouble(),
                buf.getLong(),
                buf.getLong()
            );
        }
    }

    public static Serde<DriverLocation> driverLocationSerde() {
        return Serdes.serdeFrom(new DriverLocationSerializer(), new DriverLocationDeserializer());
    }

    // ── RiderRequest ──────────────────────────────────────────────────────────

    public static class RiderRequestSerializer implements Serializer<RiderRequest> {
        @Override
        public byte[] serialize(String topic, RiderRequest data) {
            if (data == null) return null;
            byte[] idBytes = data.riderId().getBytes(StandardCharsets.UTF_8);
            var buf = ByteBuffer.allocate(4 + idBytes.length + 8 + 8 + 8 + 8);
            buf.putInt(idBytes.length);
            buf.put(idBytes);
            buf.putDouble(data.lat());
            buf.putDouble(data.lng());
            buf.putLong(data.h3Cell());
            buf.putLong(data.timestampMs());
            return buf.array();
        }
    }

    public static class RiderRequestDeserializer implements Deserializer<RiderRequest> {
        @Override
        public RiderRequest deserialize(String topic, byte[] data) {
            if (data == null) return null;
            var buf = ByteBuffer.wrap(data);
            int idLen  = buf.getInt();
            byte[] idB = new byte[idLen];
            buf.get(idB);
            return new RiderRequest(
                new String(idB, StandardCharsets.UTF_8),
                buf.getDouble(),
                buf.getDouble(),
                buf.getLong(),
                buf.getLong()
            );
        }
    }

    public static Serde<RiderRequest> riderRequestSerde() {
        return Serdes.serdeFrom(new RiderRequestSerializer(), new RiderRequestDeserializer());
    }

    private Serialization() {}
}
