package com.uberlite.lesson01.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record LocationEvent(
    @JsonProperty("driver_id") String driverId,
    @JsonProperty("latitude") double latitude,
    @JsonProperty("longitude") double longitude,
    @JsonProperty("timestamp") long timestamp
) {
    @JsonCreator
    public LocationEvent {}
}
