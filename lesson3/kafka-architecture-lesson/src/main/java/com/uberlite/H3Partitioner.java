package com.uberlite;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Partitions events by H3 hexagon index.
 * Ensures geographic co-location: same H3 region always goes to same partition.
 */
public class H3Partitioner implements Partitioner {
    
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                        Object value, byte[] valueBytes, Cluster cluster) {
        if (!(key instanceof LocationKey locationKey)) {
            throw new IllegalArgumentException("Key must be LocationKey");
        }
        
        int partitionCount = cluster.partitionCountForTopic(topic);
        long h3Index = locationKey.h3Index();
        
        // Modulo ensures even distribution across partitions
        // H3 indices are well-distributed, so no need for additional hashing
        return (int) (Math.abs(h3Index) % partitionCount);
    }
    
    @Override
    public void close() {}
    
    @Override
    public void configure(Map<String, ?> configs) {}
}
