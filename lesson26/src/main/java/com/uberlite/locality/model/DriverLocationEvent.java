package com.uberlite.locality.model;

/**
 * Immutable driver location update.
 * Produced at ~3,000 events/sec across the fleet.
 * The h3Cell field is set by the producer BEFORE sending —
 * it encodes (lat, lon) at H3 Resolution 7 and becomes the record key.
 */
public record DriverLocationEvent(
    String driverId,
    double lat,
    double lon,
    String h3Cell,      // H3 R7 hex address — the routing key
    long   timestampMs,
    String status       // "AVAILABLE" | "ON_TRIP" | "OFFLINE"
) {}
