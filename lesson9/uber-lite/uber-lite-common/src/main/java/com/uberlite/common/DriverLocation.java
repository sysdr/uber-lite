package com.uberlite.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.uber.h3core.H3Core;

import java.io.IOException;

/**
 * Immutable driver location event.
 * Uses H3 hexagonal indexing instead of raw lat/lon for spatial locality.
 * 
 * @param driverId Unique driver identifier
 * @param h3Index H3 index at resolution 9 (~174m hexagon)
 * @param latitude Original latitude (for debugging)
 * @param longitude Original longitude (for debugging)
 * @param timestamp Event time (milliseconds since epoch)
 */
public record DriverLocation(
    @JsonProperty("driver_id") String driverId,
    @JsonProperty("h3_index") long h3Index,
    @JsonProperty("latitude") double latitude,
    @JsonProperty("longitude") double longitude,
    @JsonProperty("timestamp") long timestamp
) {
    private static final H3Core h3 = createH3();
    private static final int H3_RESOLUTION = 9; // ~174m edge length

    private static H3Core createH3() {
        try {
            return H3Core.newInstance();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize H3", e);
        }
    }

    /**
     * Factory method: converts lat/lon to H3 index automatically.
     */
    public static DriverLocation of(String driverId, double lat, double lon) {
        long h3Index = h3.latLngToCell(lat, lon, H3_RESOLUTION);
        return new DriverLocation(driverId, h3Index, lat, lon, System.currentTimeMillis());
    }

    /**
     * Returns H3 hexagon center for visualization.
     */
    public double[] getH3Center() {
        var latLng = h3.cellToLatLng(h3Index);
        return new double[]{latLng.lat, latLng.lng};
    }
}
