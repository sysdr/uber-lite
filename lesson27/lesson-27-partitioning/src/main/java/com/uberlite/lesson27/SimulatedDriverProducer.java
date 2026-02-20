package com.uberlite.lesson27;

import com.uberlite.lesson27.Models.DriverLocation;
import com.uberlite.lesson27.Serialization.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * Produces simulated DriverLocation events using Virtual Threads.
 * Key: 8-byte long (H3 cell index) — consumed by H3ProducerPartitioner.
 * Rate: configurable, default 3000 events/sec across all drivers.
 */
public class SimulatedDriverProducer {

    // Real metro area bounding boxes [minLat, maxLat, minLng, maxLng]
    record MetroBounds(String city, double minLat, double maxLat, double minLng, double maxLng) {
        double randomLat(Random rng) { return minLat + rng.nextDouble() * (maxLat - minLat); }
        double randomLng(Random rng) { return minLng + rng.nextDouble() * (maxLng - minLng); }
    }

    static final List<MetroBounds> METROS = List.of(
        new MetroBounds("chicago",     41.644,  42.023, -87.940, -87.524),
        new MetroBounds("nyc",         40.477,  40.917, -74.260, -73.700),
        new MetroBounds("la",          33.700,  34.337, -118.670,-118.155),
        new MetroBounds("houston",     29.524,  30.111, -95.820, -95.069),
        new MetroBounds("phoenix",     33.290,  33.916, -112.324,-111.926),
        new MetroBounds("miami",       25.594,  25.980, -80.438, -80.118),
        new MetroBounds("denver",      39.614,  39.914, -105.110,-104.600),
        new MetroBounds("seattle",     47.494,  47.734, -122.460,-122.236)
    );

    static final int    DRIVER_COUNT   = 500;
    static final int    TARGET_EPS     = 3000;   // events per second
    static final long   INTERVAL_MS    = 1000L / (TARGET_EPS / DRIVER_COUNT); // ~600ms per driver
    static final String DRIVER_TOPIC   = TopicBootstrap.DRIVER_TOPIC;

    public static void main(String[] args) throws InterruptedException {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,    "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,  LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DriverLocationSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,     H3ProducerPartitioner.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,                  "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG,             "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,            String.valueOf(32 * 1024));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,      "lz4");
        props.put(ProducerConfig.CLIENT_ID_CONFIG,             "lesson27-driver-producer");

        var sent    = new LongAdder();
        var errors  = new LongAdder();

        try (var producer = new KafkaProducer<Long, DriverLocation>(props)) {

            // Virtual Thread per driver — 500 VTs, each ~1KB stack vs 1MB for platform threads
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {

                var rng = new Random(42L); // seeded for reproducibility
                for (int i = 0; i < DRIVER_COUNT; i++) {
                    final String driverId = "driver-%04d".formatted(i);
                    // Pin each driver to a metro — deterministic assignment
                    final MetroBounds metro = METROS.get(i % METROS.size());
                    final var driverRng = new Random(i); // per-driver seed → reproducible GPS drift

                    executor.submit(() -> {
                        while (!Thread.currentThread().isInterrupted()) {
                            double lat = metro.randomLat(driverRng);
                            double lng = metro.randomLng(driverRng);
                            var location = DriverLocation.of(driverId, lat, lng);
                            // Key is the H3 cell as a Long — H3ProducerPartitioner reads this
                            var record = new ProducerRecord<>(DRIVER_TOPIC, location.h3Cell(), location);

                            producer.send(record, (metadata, ex) -> {
                                if (ex != null) {
                                    errors.increment();
                                    System.err.println("[ERROR] " + ex.getMessage());
                                } else {
                                    sent.increment();
                                }
                            });

                            try {
                                Thread.sleep(INTERVAL_MS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    });
                }

                // Stats printer — every 5 seconds
                var statsThread = Thread.ofVirtual().start(() -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Thread.sleep(5000);
                            long s = sent.sumThenReset();
                            long e = errors.sumThenReset();
                            System.out.printf("[PRODUCER] %.1f events/sec | errors: %d%n",
                                s / 5.0, e);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });

                // Run for 5 minutes
                Thread.sleep(300_000);
                executor.shutdownNow();
                statsThread.interrupt();
            }
        }
    }
}
