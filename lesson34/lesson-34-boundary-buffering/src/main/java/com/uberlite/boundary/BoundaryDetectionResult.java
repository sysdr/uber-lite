package com.uberlite.boundary;

import java.util.Set;

/**
 * Result of boundary proximity check for a single driver position.
 * allCells() is the full set of Res-5 partition targets (1 if not near boundary, 2-3 if near).
 */
public record BoundaryDetectionResult(
    long primaryCell,
    Set<Long> allCells,
    boolean nearBoundary
) {
    /** Convenience: single-cell (no multicast needed) */
    public static BoundaryDetectionResult primary(long cell) {
        return new BoundaryDetectionResult(cell, Set.of(cell), false);
    }

    /** Multicast: driver is near boundary between multiple Res-5 cells */
    public static BoundaryDetectionResult multicast(long primary, Set<Long> all) {
        return new BoundaryDetectionResult(primary, all, true);
    }
}
