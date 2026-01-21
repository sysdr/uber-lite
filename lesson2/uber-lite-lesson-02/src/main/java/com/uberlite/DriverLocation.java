package com.uberlite;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Immutable record representing a driver's location update.
 * Uses Java 21 record syntax for zero-boilerplate data classes.
 */
public record DriverLocation(
    @JsonProperty("driver_id") String driverId,
    @JsonProperty("latitude") double latitude,
    @JsonProperty("longitude") double longitude,
    @JsonProperty("timestamp") long timestamp,
    @JsonProperty("available") boolean available
) {
    public DriverLocation {
        if (driverId == null || driverId.isBlank()) {
            throw new IllegalArgumentException("driverId cannot be null or blank");
        }
        if (latitude < -90 || latitude > 90) {
            throw new IllegalArgumentException("Invalid latitude: " + latitude);
        }
        if (longitude < -180 || longitude > 180) {
            throw new IllegalArgumentException("Invalid longitude: " + longitude);
        }
    }
}
