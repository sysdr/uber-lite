package com.uberlite.boundary;

/**
 * Immutable driver location record.
 * eventTimestamp is epoch-millis; used as dedup key component.
 */
public record DriverLocationEvent(
    String driverId,
    double lat,
    double lng,
    long eventTimestamp,
    long seed   // deterministic replay support (Lesson 22 pattern)
) {
    public static DriverLocationEvent of(String driverId, double lat, double lng, long ts) {
        return new DriverLocationEvent(driverId, lat, lng, ts, 0L);
    }
}
