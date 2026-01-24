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
        long matchLatencyMs
    ) {}

    public record DriverLocation(
        String driverId,
        double lat,
        double lon,
        long timestamp
    ) {}

    public record SpatialIndexMetrics(
        long driverLocationsIndexed,
        long matchRequestsProcessed,
        long matchesFound,
        long totalMatchLatencyMs,
        long totalDriversFound,
        long totalH3CellsSearched,
        long timestamp
    ) {}
}
