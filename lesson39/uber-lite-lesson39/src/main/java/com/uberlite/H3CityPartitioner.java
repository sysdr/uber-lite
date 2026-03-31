package com.uberlite;

import com.uber.h3core.H3Core;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Deterministic geo-based partitioner using H3 Resolution 4.
 *
 * Key format: "<entityId>|<lat>|<lon>"
 *
 * Both driver-updates and rider-requests producers use this partitioner.
 * Records from the same H3 Resolution-4 cell always land on the same
 * partition — satisfying the Kafka Streams co-partitioning contract for
 * KStream-KStream and KStream-KTable joins.
 *
 * H3 Resolution 4 cell area ≈ 1,770 km² (city-metro granularity).
 * ~50 cells cover all major Indian metro areas.
 */
public final class H3CityPartitioner implements Partitioner {

    // H3Core is thread-safe: one static instance, initialized once.
    private static final H3Core H3;
    static {
        try {
            H3 = H3Core.newInstance();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to init H3Core", e);
        }
    }

    /** City-level granularity. Resolution 4 ≈ 1,770 km² per hexagon. */
    private static final int H3_RESOLUTION = 4;

    @Override
    public int partition(String topic,
                         Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes,
                         Cluster cluster) {

        if (keyBytes == null) {
            throw new InvalidRecordException(
                "H3CityPartitioner requires a non-null key. " +
                "Records with null keys bypass geo-sharding and corrupt joins.");
        }

        var numPartitions = cluster.partitionCountForTopic(topic);
        // Parse composite key: "<entityId>|<lat>|<lon>"
        var raw = new String(keyBytes, StandardCharsets.UTF_8);
        var parts = raw.split("\\|", 3);
        if (parts.length != 3) {
            throw new InvalidRecordException(
                "Key must be '<entityId>|<lat>|<lon>', got: " + raw);
        }

        double lat, lon;
        try {
            lat = Double.parseDouble(parts[1]);
            lon = Double.parseDouble(parts[2]);
        } catch (NumberFormatException e) {
            throw new InvalidRecordException(
                "Cannot parse lat/lon from key: " + raw, e);
        }

        // O(1) bitwise H3 index computation — not a spatial search.
        var h3Index = H3.latLngToCell(lat, lon, H3_RESOLUTION);

        // Math.floorMod handles negative Long.hashCode values correctly.
        // Standard % can return negative values for negative longs.
        return Math.floorMod(Long.hashCode(h3Index), numPartitions);
    }

    /** Returns the H3 Resolution-4 index for diagnostic/metric use. */
    public static long getCityCell(double lat, double lon) {
        return H3.latLngToCell(lat, lon, H3_RESOLUTION);
    }

    /** Returns the target partition for a given lat/lon and partition count. */
    public static int getTargetPartition(double lat, double lon, int numPartitions) {
        var h3Index = H3.latLngToCell(lat, lon, H3_RESOLUTION);
        return Math.floorMod(Long.hashCode(h3Index), numPartitions);
    }

    @Override public void close() {}
    @Override public void configure(Map<String, ?> configs) {}
}
