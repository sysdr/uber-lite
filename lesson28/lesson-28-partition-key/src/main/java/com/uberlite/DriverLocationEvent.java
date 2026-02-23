package com.uberlite;

public record DriverLocationEvent(
    String driverId,
    double lat,
    double lon,
    long timestampMs,
    String status   // "AVAILABLE" | "ON_TRIP"
) {}
