package com.uberlite.lesson29;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Immutable value object for a driver location update.
 * The h3Res9 field is computed by the producer before send —
 * it is the fine-grained state store key.
 * The partition key (Res 3 parent) is derived by H3GeoPartitioner.
 */
public record DriverLocationEvent(
    @JsonProperty("driverId")   String driverId,
    @JsonProperty("lat")        double lat,
    @JsonProperty("lng")        double lng,
    @JsonProperty("h3Res9")     long   h3Res9,
    @JsonProperty("timestamp")  long   timestamp,
    @JsonProperty("status")     String status   // AVAILABLE | ON_TRIP
) {}
