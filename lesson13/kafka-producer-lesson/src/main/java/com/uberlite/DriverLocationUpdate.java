package com.uberlite;

/**
 * Immutable record representing a driver's location update.
 * Uses Java 21 record syntax for zero-boilerplate value objects.
 */
public record DriverLocationUpdate(
    String driverId,
    double lat,
    double lon,
    long timestampMs,
    float headingDegrees,
    float speedMps
) {
    /**
     * Factory method for generating synthetic location updates.
     */
    public static DriverLocationUpdate random(int index) {
        // Distribute drivers across San Francisco area
        double baseLat = 37.7749;
        double baseLon = -122.4194;
        double latOffset = (Math.random() - 0.5) * 0.1; // ~5km radius
        double lonOffset = (Math.random() - 0.5) * 0.1;
        
        return new DriverLocationUpdate(
            "driver-" + String.format("%05d", index),
            baseLat + latOffset,
            baseLon + lonOffset,
            System.currentTimeMillis(),
            (float) (Math.random() * 360.0),
            (float) (Math.random() * 25.0) // 0-25 m/s (0-90 km/h)
        );
    }
    
    /**
     * Serialize to compact binary format (40 bytes).
     */
    public byte[] toBytes() {
        var buffer = java.nio.ByteBuffer.allocate(40);
        buffer.putLong(timestampMs);
        buffer.putDouble(lat);
        buffer.putDouble(lon);
        buffer.putFloat(headingDegrees);
        buffer.putFloat(speedMps);
        // driverId stored as key, not in value
        return buffer.array();
    }
}
