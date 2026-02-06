package com.uberlite.compression;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.h3core.H3Core;

import java.io.IOException;

/**
 * Geospatial location event with H3 indexing.
 * Designed to showcase compression effectiveness on repetitive JSON field names.
 */
public record LocationEvent(
    String driverId,
    double latitude,
    double longitude,
    String h3Index,
    long timestamp,
    String status
) {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final H3Core H3 = createH3Core();

    private static H3Core createH3Core() {
        try {
            return H3Core.newInstance();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize H3", e);
        }
    }

    /**
     * Create random location event in San Francisco area
     */
    public static LocationEvent random(String driverId) {
        // SF bounds: 37.7-37.8 lat, -122.5 to -122.4 lon
        var lat = 37.7 + (Math.random() * 0.1);
        var lon = -122.5 + (Math.random() * 0.1);
        var h3Index = H3.latLngToCell(lat, lon, 9); // Resolution 9
        
        return new LocationEvent(
            driverId,
            lat,
            lon,
            Long.toHexString(h3Index),
            System.currentTimeMillis(),
            "available"
        );
    }

    /**
     * Serialize to JSON bytes
     */
    public byte[] toJson() {
        try {
            return MAPPER.writeValueAsBytes(this);
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    /**
     * Get uncompressed size
     */
    public int uncompressedSize() {
        return toJson().length;
    }
}
