package com.uberlite.concurrency;

public record DriverState(
    String driverId,
    double lat,
    double lon,
    long h3Index,
    long lastUpdateMs
) {
    public DriverState moveRandomly() {
        try {
            double deltaLat = (Math.random() - 0.5) * 0.002;
            double deltaLon = (Math.random() - 0.5) * 0.002;
            double newLat = lat + deltaLat;
            double newLon = lon + deltaLon;
            long newH3 = com.uber.h3core.H3Core.newInstance().latLngToCell(newLat, newLon, 9);
            return new DriverState(driverId, newLat, newLon, newH3, System.currentTimeMillis());
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }
}
