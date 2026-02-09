package com.uberlite.driver.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import java.util.Map;

/**
 * Partition by H3 cell, NOT driver ID.
 * Ensures spatial co-location for K-Ring neighbor queries.
 */
public class H3CellPartitioner implements Partitioner {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                        Object value, byte[] valueBytes, Cluster cluster) {
        try {
            // Parse JSON to extract h3Cell
            JsonNode node = mapper.readTree(valueBytes);
            long h3Cell = node.get("h3Cell").asLong();
            
            int numPartitions = cluster.partitionCountForTopic(topic);
            
            // Critical: Use H3 cell for partitioning
            return (int) Math.abs(h3Cell % numPartitions);
        } catch (Exception e) {
            // Fallback to round-robin if parsing fails
            return (int) (System.nanoTime() % cluster.partitionCountForTopic(topic));
        }
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}
