package com.uberlite.boundary;

import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Detects whether a driver position is within BOUNDARY_THRESHOLD of a Res-5 hex boundary.
 *
 * Algorithm (O(1), no trig):
 *   1. Convert lat/lng to H3 Res-9 cell (~174m edge length)
 *   2. gridDisk(res9Cell, KRING_RADIUS=3) → 37-cell disk, radius ~522m
 *   3. Map each disk cell to its Res-5 parent
 *   4. If the parent set has size > 1, driver is within ~522m of a Res-5 boundary
 *
 * At Res-5, cell edge length is ~9.7km. A 522m threshold means we multicast
 * only for the ~5% of drivers genuinely near a partition boundary.
 */
public final class BoundaryDetector {

    private static final Logger log = LoggerFactory.getLogger(BoundaryDetector.class);

    /** k-ring radius at Res-9. k=3 → disk radius ≈ 3 × 174m = 522m. Tune if needed. */
    public static final int KRING_BOUNDARY_RADIUS = 3;

    /** H3 resolution for partitioning (must match compound key in Lesson 32) */
    public static final int PARTITION_RESOLUTION = 5;

    /** H3 resolution for driver cell lookup (fine-grained position) */
    public static final int DRIVER_RESOLUTION = 9;

    private final H3Core h3;

    public BoundaryDetector() {
        try {
            this.h3 = H3Core.newInstance();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialise H3Core", e);
        }
    }

    /**
     * Returns the set of Res-5 partition cells this driver's event should be routed to.
     * Size 1 = no boundary proximity (normal case).
     * Size 2-3 = near boundary (multicast required).
     */
    public BoundaryDetectionResult detectTargetCells(double lat, double lng) {
        // Step 1: driver's precise Res-9 cell
        long driverRes9 = h3.latLngToCell(lat, lng, DRIVER_RESOLUTION);

        // Step 2: k-ring disk at Res-9 (37 cells for k=3)
        List<Long> diskCells = h3.gridDisk(driverRes9, KRING_BOUNDARY_RADIUS);

        // Step 3: map each disk cell to its Res-5 ancestor (pure bit operation, ~1ns each)
        Set<Long> res5Parents = diskCells.stream()
                .map(cell -> h3.cellToParent(cell, PARTITION_RESOLUTION))
                .collect(Collectors.toSet());

        // Step 4: the driver's own Res-5 cell is the primary
        long primaryRes5 = h3.cellToParent(driverRes9, PARTITION_RESOLUTION);

        if (res5Parents.size() == 1) {
            return BoundaryDetectionResult.primary(primaryRes5);
        }

        log.debug("Boundary detected for ({},{}) → {} Res-5 targets: {}",
                lat, lng, res5Parents.size(), res5Parents);

        return BoundaryDetectionResult.multicast(primaryRes5, res5Parents);
    }

    /**
     * Compute partition assignment from a Res-5 cell + driverId.
     * Must mirror the compound key logic from Lesson 32.
     */
    public static String toPartitionKey(long res5Cell, String driverId) {
        int subShard = Math.abs(driverId.hashCode()) % 8;
        return res5Cell + "|" + subShard;
    }

    public H3Core h3() { return h3; }
}
