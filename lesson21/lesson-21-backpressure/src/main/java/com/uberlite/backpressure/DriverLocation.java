package com.uberlite.backpressure;

public record DriverLocation(
    String driverId,
    double latitude,
    double longitude,
    long timestamp,
    String status
) {
    public byte[] toBytes() {
        return String.format("%s,%.6f,%.6f,%d,%s", 
            driverId, latitude, longitude, timestamp, status).getBytes();
    }
}
