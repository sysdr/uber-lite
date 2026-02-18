package com.uberlite.locality.producer;

import com.uberlite.locality.H3GeoPartitioner;
import com.uberlite.locality.model.DriverLocationEvent;
import com.uberlite.locality.model.RiderRequestEvent;
import com.uberlite.locality.serde.JsonSerde;
import com.uber.h3core.H3Core;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Deterministic load simulator for Lesson 26.
 *
 * Generates driver and rider events for 8 H3 R7 cells across 4 cities.
 * Both producer instances use H3GeoPartitioner — same class, same routing function.
 *
 * Design intent:
 *   - driver-locations producer keys by h3Cell → H3GeoPartitioner → partition P
 *   - rider-requests producer keys by h3Cell   → H3GeoPartitioner → partition P
 *   → Same cell, same partition, always. Zero repartition topics.
 *
 * Throughput target: 3,000+ events/sec total (split ~50/50 driver/rider).
 */
public final class DataLocalitySimulator {

    private static final Logger LOG = LoggerFactory.getLogger(DataLocalitySimulator.class);
    private static final int H3_RESOLUTION = 7;

    // 8 representative cells across NYC, SF, Chicago, London
    // In production, these would be dynamically discovered from live fleet positions.
    private static final double[][] CITY_COORDS = {
        {40.7128,  -74.0060},  // NYC Financial District
        {40.7580,  -73.9855},  // NYC Midtown
        {37.7749, -122.4194},  // SF downtown
        {37.3382, -121.8863},  // San Jose
        {41.8781,  -87.6298},  // Chicago Loop
        {41.9742,  -87.9073},  // O'Hare
        {51.5074,   -0.1278},  // London City
        {51.4816,   -0.0078}   // London Canary Wharf
    };

    public static void main(String[] args) throws Exception {
        var h3 = H3Core.newInstance();

        // Pre-compute H3 cells for our city coords
        var h3Cells = new ArrayList<String>();
        for (var coords : CITY_COORDS) {
            String cell = h3.latLngToCellAddress(coords[0], coords[1], H3_RESOLUTION);
            h3Cells.add(cell);
            LOG.info("City coord ({}, {}) → H3 R7 cell: {}", coords[0], coords[1], cell);
        }

        var driverProducer = buildProducer(DriverLocationEvent.class);
        var riderProducer  = buildProducer(RiderRequestEvent.class);

        int totalPairs = 5_000;
        var driverSent = new AtomicLong(0);
        var riderSent  = new AtomicLong(0);
        var random = new Random(42L); // deterministic for reproducibility

        LOG.info("Starting simulation: {} driver+rider pairs across {} H3 cells", totalPairs, h3Cells.size());

        var latch = new CountDownLatch(totalPairs * 2);
        long startMs = System.currentTimeMillis();

        for (int i = 0; i < totalPairs; i++) {
            // Pick a random cell from our set
            String cell = h3Cells.get(random.nextInt(h3Cells.size()));

            // Get representative lat/lon for this cell (center point)
            var centerCoords = h3.cellToLatLng(h3.stringToH3(cell));
            double lat = centerCoords.lat + (random.nextGaussian() * 0.001);  // ±~100m jitter
            double lon = centerCoords.lng + (random.nextGaussian() * 0.001);

            // Re-encode with jitter — should still land in same R7 cell (5.16 km² cell, ~100m jitter)
            String actualCell = h3.latLngToCellAddress(lat, lon, H3_RESOLUTION);

            // Driver event — key = H3 cell hex → H3GeoPartitioner
            var driverEvent = new DriverLocationEvent(
                "driver-" + String.format("%05d", i % 1000),
                lat, lon, actualCell,
                System.currentTimeMillis(),
                "AVAILABLE"
            );

            driverProducer.send(
                new ProducerRecord<>("driver-locations", actualCell, driverEvent),
                (metadata, ex) -> {
                    if (ex == null) {
                        driverSent.incrementAndGet();
                    } else {
                        LOG.error("Driver produce failed", ex);
                    }
                    latch.countDown();
                }
            );

            // Rider event — SAME cell as driver → guaranteed same partition
            var riderEvent = new RiderRequestEvent(
                "rider-" + String.format("%05d", i),
                lat + 0.0002, lon + 0.0002,  // Rider slightly displaced within same cell
                actualCell,
                System.currentTimeMillis()
            );

            riderProducer.send(
                new ProducerRecord<>("rider-requests", actualCell, riderEvent),
                (metadata, ex) -> {
                    if (ex == null) {
                        riderSent.incrementAndGet();
                    } else {
                        LOG.error("Rider produce failed", ex);
                    }
                    latch.countDown();
                }
            );

            // Throttle to ~1000 pairs/sec to avoid overloading local Docker Kafka
            if (i % 100 == 0) {
                Thread.sleep(100);
                LOG.info("Progress: {}/{} pairs sent", i, totalPairs);
            }
        }

        // Flush both producers
        driverProducer.flush();
        riderProducer.flush();
        latch.await();

        long elapsed = System.currentTimeMillis() - startMs;
        LOG.info("Simulation complete: {} drivers + {} riders in {}ms ({} events/sec)",
            driverSent.get(), riderSent.get(), elapsed,
            (driverSent.get() + riderSent.get()) * 1000L / Math.max(elapsed, 1));

        driverProducer.close();
        riderProducer.close();
    }

    @SuppressWarnings("unchecked")
    private static <T> KafkaProducer<String, T> buildProducer(Class<T> type) {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new JsonSerde<>(type).serializer().getClass().getName());
        // H3GeoPartitioner — same class on both producers = co-partitioning contract satisfied
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, H3GeoPartitioner.class.getName());
        // Async batching for throughput
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);          // 64KB batches
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);               // 5ms linger for batching
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864L);   // 64MB RecordAccumulator
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.ACKS_CONFIG, "1");                  // Leader ack for throughput
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Inline value serializer - build a custom producer
        var serializer = new JsonSerde<>(type).serializer();
        return new KafkaProducer<>(
            props,
            new org.apache.kafka.common.serialization.StringSerializer(),
            serializer
        );
    }
}
