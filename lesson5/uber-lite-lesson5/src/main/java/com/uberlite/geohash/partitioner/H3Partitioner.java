package com.uberlite.geohash.partitioner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.h3core.H3Core;
import com.uberlite.geohash.model.DriverLocation;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class H3Partitioner implements Partitioner {
    private static final int H3_RESOLUTION = 9;
    private static final H3Core h3;
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        try {
            h3 = H3Core.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize H3", e);
        }
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                        Object value, byte[] valueBytes, Cluster cluster) {
        try {
            var location = mapper.readValue(valueBytes, DriverLocation.class);
            long h3Index = h3.latLngToCell(location.lat(), location.lon(), H3_RESOLUTION);
            
            int partitionCount = cluster.partitionCountForTopic(topic);
            return (int) (Math.abs(h3Index) % partitionCount);
        } catch (Exception e) {
            return 0;
        }
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}
