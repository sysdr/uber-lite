package com.uberlite;

public record DriverLocation(
    String driverId,
    long h3Index,
    double lat,
    double lon,
    long timestamp
) {}
