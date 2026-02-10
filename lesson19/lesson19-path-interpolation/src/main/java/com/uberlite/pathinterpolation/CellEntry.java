package com.uberlite.pathinterpolation;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CellEntry(
    @JsonProperty("driver_id") String driverId,
    @JsonProperty("timestamp") long timestamp,
    @JsonProperty("current_cell") long currentCell,
    @JsonProperty("interpolated") boolean interpolated
) {}
