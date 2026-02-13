package com.uberlite.model;

import java.time.Instant;

public record DriverState(
    String driverId,
    long h3Index,
    Instant timestamp,
    long seed
) {
    public static DriverState create(String driverId, long h3Index, long seed) {
        return new DriverState(driverId, h3Index, Instant.now(), seed);
    }
}
