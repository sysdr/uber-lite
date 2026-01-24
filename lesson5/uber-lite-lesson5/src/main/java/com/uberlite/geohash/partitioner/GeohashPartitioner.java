package com.uberlite.geohash.partitioner;

import ch.hsr.geohash.GeoHash;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.uberlite.geohash.model.DriverLocation;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class GeohashPartitioner implements Partitioner {
    private static final int PRECISION = 6;
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                        Object value, byte[] valueBytes, Cluster cluster) {
        try {
            var location = mapper.readValue(valueBytes, DriverLocation.class);
            String geohash = GeoHash.withCharacterPrecision(
                location.lat(), location.lon(), PRECISION
            ).toBase32();
            
            int partitionCount = cluster.partitionCountForTopic(topic);
            return Math.abs(geohash.hashCode()) % partitionCount;
        } catch (Exception e) {
            return 0; // Fallback
        }
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}
