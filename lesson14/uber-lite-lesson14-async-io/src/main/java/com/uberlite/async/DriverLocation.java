package com.uberlite.async;

import com.fasterxml.jackson.annotation.JsonProperty;

public record DriverLocation(
    @JsonProperty("driver_id") String driverId,
    @JsonProperty("latitude") double latitude,
    @JsonProperty("longitude") double longitude,
    @JsonProperty("timestamp") long timestamp,
    @JsonProperty("heading") int heading,
    @JsonProperty("speed_kmh") double speedKmh
) {
    public static DriverLocation random(String driverId) {
        // Simulate movement in San Francisco area
        double lat = 37.7749 + (Math.random() - 0.5) * 0.1;
        double lon = -122.4194 + (Math.random() - 0.5) * 0.1;
        int heading = (int) (Math.random() * 360);
        double speed = 20 + Math.random() * 40; // 20-60 km/h
        
        return new DriverLocation(
            driverId,
            lat,
            lon,
            System.currentTimeMillis(),
            heading,
            speed
        );
    }
}
