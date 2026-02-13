package com.uberlite.metrics;

public record DriverLocation(
    String driverId,
    double latitude,
    double longitude,
    long timestamp,
    String hexId
) {}
