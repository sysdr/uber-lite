package com.uberlite.lesson29;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class H3GeoPartitionerTest {

    @Test
    void partitionerReturnsValidPartitionForValidH3Key() {
        var p = new H3GeoPartitioner();
        p.configure(Collections.emptyMap());
        Cluster cluster = clusterWithPartitions(60);
        // Valid 8-byte H3 index (big-endian long)
        long h3Index = 0x832830fffffffffL;
        byte[] keyBytes = ByteBuffer.allocate(8).putLong(h3Index).array();
        int partition = p.partition(TopicAdmin.DRIVER_LOCATIONS_TOPIC, null, keyBytes, null, new byte[0], cluster);
        assertTrue(partition >= 0 && partition < 60);
        p.close();
    }

    @Test
    void fallbackForNullKeyBytes() {
        var p = new H3GeoPartitioner();
        p.configure(Collections.emptyMap());
        Cluster cluster = clusterWithPartitions(60);
        int partition = p.partition(TopicAdmin.DRIVER_LOCATIONS_TOPIC, null, null, null, "value".getBytes(), cluster);
        assertTrue(partition >= 0 && partition < 60);
        p.close();
    }

    @Test
    void fallbackForShortKeyBytes() {
        var p = new H3GeoPartitioner();
        p.configure(Collections.emptyMap());
        Cluster cluster = clusterWithPartitions(60);
        byte[] shortKey = new byte[4];
        int partition = p.partition(TopicAdmin.DRIVER_LOCATIONS_TOPIC, null, shortKey, null, new byte[0], cluster);
        assertTrue(partition >= 0 && partition < 60);
        p.close();
    }

    private static Cluster clusterWithPartitions(int numPartitions) {
        Node node = new Node(1, "localhost", 9092);
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++)
            partitions.add(new PartitionInfo(TopicAdmin.DRIVER_LOCATIONS_TOPIC, i, node, null, null));
        return new Cluster("cluster", Collections.singletonList(node), partitions, Collections.emptySet(), Collections.emptySet());
    }
}
