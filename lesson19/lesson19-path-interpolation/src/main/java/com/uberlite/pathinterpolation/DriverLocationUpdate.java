package com.uberlite.pathinterpolation;

import com.fasterxml.jackson.annotation.JsonProperty;

public record DriverLocationUpdate(
    @JsonProperty("driver_id") String driverId,
    @JsonProperty("h3_cell") long h3Cell,
    @JsonProperty("timestamp") long timestamp,
    @JsonProperty("latitude") double latitude,
    @JsonProperty("longitude") double longitude
) {}
