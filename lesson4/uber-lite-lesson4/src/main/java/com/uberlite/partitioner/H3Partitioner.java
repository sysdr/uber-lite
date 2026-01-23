package com.uberlite.partitioner;

import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.common.Cluster;

public class H3Partitioner implements StreamPartitioner<Long, com.uberlite.model.DriverLocation> {
    
    @Override
    public Integer partition(String topic, Long key, com.uberlite.model.DriverLocation value, int numPartitions) {
        if (key == null) {
            throw new IllegalArgumentException("Key must be Long H3 index");
        }
        
        // Use Long.hashCode to preserve spatial locality
        return Math.abs(Long.hashCode(key)) % numPartitions;
    }
}
