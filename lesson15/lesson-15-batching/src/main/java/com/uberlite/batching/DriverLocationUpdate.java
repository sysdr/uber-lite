package com.uberlite.batching;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.util.UUID;

/**
 * Immutable record representing a driver's location update.
 * Serialized size: ~200 bytes (UUID=36, H3=16, timestamp=8, heading=4, speed=4, status=10, padding=122)
 */
public record DriverLocationUpdate(
    @JsonProperty("driverId") UUID driverId,
    @JsonProperty("h3Index") long h3Index,
    @JsonProperty("timestamp") long timestamp,
    @JsonProperty("heading") double heading,
    @JsonProperty("speed") double speed,
    @JsonProperty("status") DriverStatus status
) {
    public enum DriverStatus {
        AVAILABLE, EN_ROUTE_TO_PICKUP, ON_TRIP, OFFLINE
    }
    
    public static DriverLocationUpdate create(UUID driverId, double lat, double lon) {
        try {
            long h3Index = com.uber.h3core.H3Core.newInstance().latLngToCell(lat, lon, 9);
            return new DriverLocationUpdate(
                driverId,
                h3Index,
                System.currentTimeMillis(),
                Math.random() * 360,
                20 + Math.random() * 40,
                DriverStatus.AVAILABLE
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to compute H3 index", e);
        }
    }
}
