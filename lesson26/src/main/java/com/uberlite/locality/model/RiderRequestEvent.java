package com.uberlite.locality.model;

/**
 * Rider match request.
 * Keyed by H3 R7 cell of pickup location — same partitioning scheme
 * as driver-locations, guaranteeing co-location on the same partition.
 */
public record RiderRequestEvent(
    String riderId,
    double pickupLat,
    double pickupLon,
    String h3Cell,       // H3 R7 hex address — same key scheme as driver
    long   timestampMs
) {}
