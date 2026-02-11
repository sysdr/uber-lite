package com.uberlite.concurrency;

import com.fasterxml.jackson.annotation.JsonProperty;

public record DriverLocationUpdate(
    @JsonProperty("driver_id") String driverId,
    @JsonProperty("lat") double lat,
    @JsonProperty("lon") double lon,
    @JsonProperty("h3_index") long h3Index,
    @JsonProperty("timestamp_ms") long timestampMs
) {}
