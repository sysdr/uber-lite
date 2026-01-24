package com.uberlite.geohash.model;

public record DriverLocation(
    String driverId,
    double lat,
    double lon,
    long timestamp,
    String cityName
) {
    public int latitudeBand() {
        return (int) ((lat + 90) / 10); // 0-17 bands (10° each)
    }
}
