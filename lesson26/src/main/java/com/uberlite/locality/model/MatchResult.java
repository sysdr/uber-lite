package com.uberlite.locality.model;

/**
 * Output of a successful driver-rider match.
 * partition field is captured at match time to prove co-location.
 */
public record MatchResult(
    String riderId,
    String driverId,
    String h3Cell,
    int    partition,      // Must equal both input record partitions — proves locality
    long   matchLatencyMs, // Time from rider request to match output
    long   timestampMs
) {}
