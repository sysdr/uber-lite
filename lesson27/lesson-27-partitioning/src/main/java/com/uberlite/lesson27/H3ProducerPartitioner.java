package com.uberlite.lesson27;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import java.util.Map;
import java.nio.ByteBuffer;

/**
 * Kafka Producer-side Partitioner (kafka-clients API).
 *
 * This is the PRODUCER side of the co-partitioning contract.
 * The Kafka Streams StreamPartitioner (below) is the CONSUMER/TOPOLOGY side.
 * Both must produce identical partition assignments for the same h3Cell value.
 *
 * Wire contract: the record key is an 8-byte big-endian long representing
 * the H3 Resolution-6 cell index.
 */
public class H3ProducerPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        var partitions  = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        if (keyBytes == null || keyBytes.length != 8) {
            // Fallback: default murmur2 on whatever key we have
            return Math.abs(key.hashCode()) % numPartitions;
        }

        long h3Cell = ByteBuffer.wrap(keyBytes).getLong();
        return H3Util.partitionFor(h3Cell, numPartitions);
    }

    @Override public void close() {}
    @Override public void configure(Map<String, ?> configs) {}
}
