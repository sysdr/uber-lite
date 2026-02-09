package com.uberlite.driver.model;

import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import java.time.Instant;

/**
 * H3-native Driver model.
 * Partition key is h3Cell, NOT driver ID—critical for spatial co-location.
 */
public record Driver(
    String id,
    long h3Cell,        // H3 index at Resolution 9
    int heading,        // 0-359 degrees
    double speedMps,    // meters per second
    Instant timestamp
) {
    private static final H3Core h3;
    private static final int REQUIRED_RESOLUTION = 9;
    private static final double MAX_SPEED_MPS = 33.3; // ~120 km/h

    static {
        try {
            h3 = H3Core.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize H3", e);
        }
    }

    public Driver {
        // Validate H3 resolution
        if (h3.getResolution(h3Cell) != REQUIRED_RESOLUTION) {
            throw new IllegalArgumentException(
                "Driver H3 cell must be Resolution " + REQUIRED_RESOLUTION + 
                ", got: " + h3.getResolution(h3Cell)
            );
        }
        
        // Validate heading
        if (heading < 0 || heading >= 360) {
            throw new IllegalArgumentException("Heading must be [0, 360), got: " + heading);
        }
        
        // Validate speed
        if (speedMps < 0 || speedMps > MAX_SPEED_MPS) {
            throw new IllegalArgumentException(
                "Speed must be [0, " + MAX_SPEED_MPS + "] m/s, got: " + speedMps
            );
        }
    }

    /**
     * Move driver in current heading direction.
     * Returns new Driver in adjacent H3 cell if boundary crossed.
     */
    public Driver move(long elapsedSeconds) {
        try {
            double distanceMeters = speedMps * elapsedSeconds;
            
            // Get current cell center
            LatLng currentCenter = h3.cellToLatLng(h3Cell);
            
            // Project forward (simple great-circle approximation)
            LatLng newPosition = project(currentCenter, heading, distanceMeters);
            
            // Convert to H3 cell (may be same cell if movement is small)
            long newCell = h3.latLngToCell(newPosition.lat, newPosition.lng, REQUIRED_RESOLUTION);
            
            return new Driver(id, newCell, heading, speedMps, Instant.now());
        } catch (Exception e) {
            throw new RuntimeException("Failed to move driver: " + id, e);
        }
    }

    /**
     * Simple great-circle projection.
     * Production systems use Vincenty or Karney algorithms.
     */
    private LatLng project(LatLng start, double bearingDegrees, double distanceMeters) {
        double earthRadiusMeters = 6371000.0;
        double angularDistance = distanceMeters / earthRadiusMeters;
        double bearingRadians = Math.toRadians(bearingDegrees);
        
        double lat1 = Math.toRadians(start.lat);
        double lon1 = Math.toRadians(start.lng);
        
        double lat2 = Math.asin(
            Math.sin(lat1) * Math.cos(angularDistance) +
            Math.cos(lat1) * Math.sin(angularDistance) * Math.cos(bearingRadians)
        );
        
        double lon2 = lon1 + Math.atan2(
            Math.sin(bearingRadians) * Math.sin(angularDistance) * Math.cos(lat1),
            Math.cos(angularDistance) - Math.sin(lat1) * Math.sin(lat2)
        );
        
        return new LatLng(Math.toDegrees(lat2), Math.toDegrees(lon2));
    }

    public LatLng getLatLng() {
        try {
            return h3.cellToLatLng(h3Cell);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert H3 cell to LatLng", e);
        }
    }
}
