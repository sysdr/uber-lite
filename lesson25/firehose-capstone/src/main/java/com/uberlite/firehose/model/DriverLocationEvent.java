package com.uberlite.firehose.model;

/**
 * Immutable record representing a single driver GPS ping.
 * Serialized as 28 bytes: [4 int driverId][8 double lat][8 double lon][8 long timestampMs]
 */
public record DriverLocationEvent(int driverId, double lat, double lon, long timestampMs) {

    public static final int BYTE_SIZE = Integer.BYTES + Double.BYTES + Double.BYTES + Long.BYTES; // 28

    public static byte[] serialize(DriverLocationEvent e) {
        var buf = java.nio.ByteBuffer.allocate(BYTE_SIZE);
        buf.putInt(e.driverId());
        buf.putDouble(e.lat());
        buf.putDouble(e.lon());
        buf.putLong(e.timestampMs());
        return buf.array();
    }

    public static DriverLocationEvent deserialize(byte[] bytes) {
        var buf = java.nio.ByteBuffer.wrap(bytes);
        int driverId = buf.getInt();
        double lat   = buf.getDouble();
        double lon   = buf.getDouble();
        long ts      = buf.getLong();
        return new DriverLocationEvent(driverId, lat, lon, ts);
    }
}
