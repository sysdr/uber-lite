package com.uberlite.lesson27;

/**
 * Canonical domain records for Lesson 27.
 * Records give us: equals, hashCode, toString, and compact serialization targets for free.
 */
public final class Models {

    public record DriverLocation(
        String  driverId,
        double  lat,
        double  lng,
        long    h3Cell,       // Resolution-6 cell — pre-computed at source
        long    timestampMs
    ) {
        /** Factory: compute H3 at construction time, not at partition time */
        public static DriverLocation of(String driverId, double lat, double lng) {
            return new DriverLocation(
                driverId, lat, lng,
                H3Util.cellOf(lat, lng),
                System.currentTimeMillis()
            );
        }
    }

    public record RiderRequest(
        String  riderId,
        double  lat,
        double  lng,
        long    h3Cell,
        long    timestampMs
    ) {
        public static RiderRequest of(String riderId, double lat, double lng) {
            return new RiderRequest(
                riderId, lat, lng,
                H3Util.cellOf(lat, lng),
                System.currentTimeMillis()
            );
        }
    }

    public record MatchEvent(
        String  riderId,
        String  driverId,
        long    h3Cell,
        long    latencyMs
    ) {}

    private Models() {}
}
