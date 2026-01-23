package com.uberlite.model;

public record DriverLocation(
    long driverId,
    double lat,
    double lon,
    long timestamp,
    String status
) {}
