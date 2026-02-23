package com.uberlite;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class H3GeoPartitionerTest {
    @Test
    void partitionerUsesH3ForValidCoords() {
        var p = new H3GeoPartitioner();
        p.configure(Collections.emptyMap());
        Cluster cluster = clusterWithPartitions(64);
        byte[] valueBytes = new byte[32];
        java.nio.ByteBuffer.wrap(valueBytes).putDouble(41.8781).putDouble(-87.6298);
        int partition = p.partition("driver-locations", "driver-0", null, null, valueBytes, cluster);
        assertTrue(partition >= 0 && partition < 64);
        p.close();
    }

    @Test
    void fallbackForShortValue() {
        var p = new H3GeoPartitioner();
        p.configure(Collections.emptyMap());
        Cluster cluster = clusterWithPartitions(64);
        byte[] shortValue = new byte[8];
        int partition = p.partition("driver-locations", "driver-0", "key".getBytes(), null, shortValue, cluster);
        assertTrue(partition >= 0 && partition < 64);
        assertTrue(p.getFallbackCount() >= 1);
        p.close();
    }

    private static Cluster clusterWithPartitions(int numPartitions) {
        Node node = new Node(1, "localhost", 9092);
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++)
            partitions.add(new PartitionInfo("driver-locations", i, node, null, null));
        return new Cluster("cluster", Collections.singletonList(node), partitions, Collections.emptySet(), Collections.emptySet());
    }
}
