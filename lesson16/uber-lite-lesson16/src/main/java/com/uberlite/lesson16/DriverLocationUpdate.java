package com.uberlite.lesson16;

import com.fasterxml.jackson.annotation.JsonProperty;

public record DriverLocationUpdate(
    @JsonProperty("driver_id") String driverId,
    @JsonProperty("latitude") double latitude,
    @JsonProperty("longitude") double longitude,
    @JsonProperty("h3_index") long h3Index,
    @JsonProperty("timestamp") long timestamp,
    @JsonProperty("speed_mph") double speedMph,
    @JsonProperty("heading") int heading,
    @JsonProperty("status") String status
) {
    public static DriverLocationUpdate create(String driverId, double lat, double lon, long h3Index) {
        return new DriverLocationUpdate(
            driverId,
            lat,
            lon,
            h3Index,
            System.currentTimeMillis(),
            Math.random() * 60,  // 0-60 mph
            (int)(Math.random() * 360),  // 0-359 degrees
            "available"
        );
    }
}
