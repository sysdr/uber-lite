package com.uberlite.lesson29;

import com.uber.h3core.H3Core;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Routes Kafka records to partitions using H3 Resolution 3 cell indices.
 *
 * Contract:
 *   - Key MUST be 8-byte big-endian long representing an H3 index at any resolution.
 *   - The partitioner coarsens the index to Res 3 before hashing.
 *   - partition = Math.abs(Long.hashCode(res3Index)) % numPartitions
 *
 * Thread safety: H3Core is thread-safe after initialization. The static initializer
 * ensures a single instance shared across all producer I/O threads.
 */
public final class H3GeoPartitioner implements Partitioner {

    private static final H3Core H3;
    private static final int ROUTING_RESOLUTION = 3;

    static {
        try {
            H3 = H3Core.newInstance();
        } catch (IOException e) {
            throw new ExceptionInInitializerError(
                "Failed to load H3 native library: " + e.getMessage());
        }
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {

        int numPartitions = cluster.partitionCountForTopic(topic);

        if (keyBytes == null || keyBytes.length != Long.BYTES) {
            // No valid H3 key — fall back to uniform murmur2 distribution
            byte[] fallbackBytes = (valueBytes != null) ? valueBytes : new byte[0];
            return Utils.toPositive(Utils.murmur2(fallbackBytes)) % numPartitions;
        }

        long h3Index = ByteBuffer.wrap(keyBytes).getLong();

        // Coarsen to Res 3 — this is the geographic routing boundary.
        // Two events from Res 9 cells within the same Res 3 parent will
        // always produce the same res3Index, and thus the same partition.
        long res3Index = H3.cellToParent(h3Index, ROUTING_RESOLUTION);

        // Java's % operator returns negative values for negative operands.
        // Math.abs + Long.hashCode gives us a stable positive int.
        int partitionId = Math.abs(Long.hashCode(res3Index)) % numPartitions;

        return partitionId;
    }

    @Override
    public void close() {
        // H3Core holds no closeable resources; H3 native lib is process-scoped.
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No per-instance configuration required.
        // numPartitions is read dynamically from Cluster metadata at call time.
    }
}
