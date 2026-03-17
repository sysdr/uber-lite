package com.uberlite.boundary;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BoundaryDetectorTest {

    @Test
    void primaryCellSingleCell() {
        BoundaryDetector d = new BoundaryDetector();
        BoundaryDetectionResult r = d.detectTargetCells(37.7749, -122.4194);
        assertNotNull(r);
        assertTrue(r.allCells().size() >= 1);
        assertTrue(r.primaryCell() != 0);
    }

    @Test
    void toPartitionKeyFormat() {
        String key = BoundaryDetector.toPartitionKey(0x85283473fffffffL, "driver-1");
        assertTrue(key.contains("|"));
        String[] parts = key.split("\\|");
        assertEquals(2, parts.length);
        assertTrue(Long.parseLong(parts[0]) != 0);
        int subShard = Integer.parseInt(parts[1]);
        assertTrue(subShard >= 0 && subShard < 8);
    }
}
