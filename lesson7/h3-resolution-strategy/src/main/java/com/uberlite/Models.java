package com.uberlite;

public class Models {
    public record DriverLocationUpdate(
        String driverId,
        double lat,
        double lon,
        long timestamp
    ) {}

    public record RiderMatchRequest(
        String riderId,
        double lat,
        double lon,
        long timestamp
    ) {}

    public record MatchResult(
        String riderId,
        String driverId,
        double driverLat,
        double driverLon,
        double distanceKm,
        long matchLatencyMs,
        int resolutionUsed
    ) {}

    public record DriverLocation(
        String driverId,
        double lat,
        double lon,
        long timestamp
    ) {}

    public record ResolutionBenchmarkResult(
        int resolution,
        long cellCount,
        long memoryBytes,
        long p50LatencyNanos,
        long p99LatencyNanos,
        double emptyRatio,
        long timestamp
    ) {}
}
