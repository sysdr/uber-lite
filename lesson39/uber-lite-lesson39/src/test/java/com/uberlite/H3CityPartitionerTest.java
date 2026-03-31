package com.uberlite;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class H3CityPartitionerTest {

    @Test
    void sameLocationDifferentEntityYieldsSamePartition() {
        int n = TopicAdmin.TARGET_PARTITIONS;
        double lat = 19.0760, lon = 72.8777;
        int d = H3CityPartitioner.getTargetPartition(lat, lon, n);
        int r = H3CityPartitioner.getTargetPartition(lat, lon, n);
        assertEquals(d, r);
    }

    @Test
    void partitionInRange() {
        int n = 60;
        int p = H3CityPartitioner.getTargetPartition(12.97, 77.59, n);
        assertEquals(p, Math.floorMod(p, n));
        org.junit.jupiter.api.Assertions.assertTrue(p >= 0 && p < n);
    }
}
