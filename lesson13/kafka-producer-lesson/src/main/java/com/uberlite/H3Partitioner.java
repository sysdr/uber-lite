package com.uberlite;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Custom partitioner that uses H3 cell indices for uniform spatial distribution.
 * 
 * Key insight: Geohash-based partitioning causes hot spots at high latitudes
 * due to meridian convergence. H3 hexagons are uniform size globally.
 * 
 * At resolution 9 (~0.1km²), we achieve even partition distribution regardless
 * of geographic density variations (Manhattan vs Wyoming).
 */
public class H3Partitioner implements Partitioner {
    private static final Logger log = LoggerFactory.getLogger(H3Partitioner.class);
    
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                        Object value, byte[] valueBytes, Cluster cluster) {
        if (!(key instanceof Long h3Index)) {
            throw new IllegalArgumentException("Key must be Long H3 index, got: " + 
                (key != null ? key.getClass() : "null"));
        }
        
        int numPartitions = cluster.partitionCountForTopic(topic);
        
        // Use modulo for deterministic partition assignment
        // H3 indices are uniformly distributed, so this gives even load
        int partition = (int) Math.abs(h3Index % numPartitions);
        
        log.trace("H3 index {} → partition {}/{}", 
            Long.toHexString(h3Index), partition, numPartitions);
        
        return partition;
    }
    
    @Override
    public void close() {
        // No resources to clean up
    }
    
    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed
    }
}
