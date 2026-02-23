package com.uberlite;

import com.uber.h3core.H3Core;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Simulates 500 drivers across 10 cities, each producing location updates
 * at ~10 events/sec using Virtual Threads.
 *
 * Demonstrates that H3 Res-3 routes drivers in the same city to the same partition.
 */
public class L28ProducerMain {

    // 10 cities: [name, lat, lon]
    private static final double[][] CITIES = {
        // lat, lon
        {41.8781, -87.6298},  // Chicago
        {40.7128, -74.0060},  // New York
        {34.0522, -118.2437}, // Los Angeles
        {29.7604, -95.3698},  // Houston
        {33.4484, -112.0740}, // Phoenix
        {47.6062, -122.3321}, // Seattle
        {25.7617, -80.1918},  // Miami
        {37.7749, -122.4194}, // San Francisco
        {39.9526, -75.1652},  // Philadelphia
        {42.3601, -71.0589}   // Boston
    };

    private static final String TOPIC = "driver-locations";
    private static final int DRIVERS_PER_CITY = 50;
    private static final int EVENTS_PER_DRIVER = 20;
    private static final AtomicLong sentCount = new AtomicLong(0);
    private static final AtomicLong errorCount = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        TopicAdmin.ensureTopic();

        H3Core h3 = H3Core.newInstance();

        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DriverLocationSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, H3GeoPartitioner.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        try (var producer = new KafkaProducer<String, DriverLocationEvent>(props)) {
            // Virtual Thread per driver — 500 threads at ~1KB each vs 500MB for platform threads
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var futures = new ArrayList<Future<?>>();

                for (int cityIdx = 0; cityIdx < CITIES.length; cityIdx++) {
                    final double baseLat = CITIES[cityIdx][0];
                    final double baseLon = CITIES[cityIdx][1];
                    final long cityH3 = h3.latLngToCell(baseLat, baseLon, 3);
                    final String cityCell = Long.toHexString(cityH3);

                    for (int d = 0; d < DRIVERS_PER_CITY; d++) {
                        final String driverId = "driver-%d-%d".formatted(cityIdx, d);
                        final int driverNum = d;

                        futures.add(executor.submit(() -> {
                            var rng = new Random(driverId.hashCode()); // seeded for reproducibility
                            for (int i = 0; i < EVENTS_PER_DRIVER; i++) {
                                // Jitter within ~2km of city center
                                double lat = baseLat + (rng.nextDouble() - 0.5) * 0.02;
                                double lon = baseLon + (rng.nextDouble() - 0.5) * 0.02;

                                var event = new DriverLocationEvent(
                                    driverId, lat, lon,
                                    System.currentTimeMillis(),
                                    "AVAILABLE"
                                );

                                producer.send(
                                    new ProducerRecord<>(TOPIC, driverId, event),
                                    (meta, ex) -> {
                                        if (ex != null) {
                                            errorCount.incrementAndGet();
                                        } else {
                                            sentCount.incrementAndGet();
                                            if (sentCount.get() % 1000 == 0) {
                                                System.out.printf(
                                                    "[Producer] Sent=%d City-H3=%s → Partition=%d%n",
                                                    sentCount.get(), cityCell, meta.partition()
                                                );
                                            }
                                        }
                                    }
                                );
                                // ~10 events/sec per driver
                                Thread.sleep(100);
                            }
                            return null;
                        }));
                    }
                }

                for (var f : futures) f.get();
            }

            producer.flush();
        }

        System.out.printf("%n[Producer] Done. Sent=%d Errors=%d%n",
            sentCount.get(), errorCount.get());
    }
}
