package com.uberlite.lesson29;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.h3core.H3Core;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Demonstrates the H3GeoPartitioner by simulating 100 drivers
 * across 6 metropolitan H3 Res 3 cells, sending 60 updates each.
 *
 * Virtual Threads are used for concurrent producer sends —
 * each driver gets its own lightweight thread without platform thread starvation.
 *
 * After sending, the demo consumes partition metadata from the broker
 * and prints a distribution report.
 */
public class PartitionerDemo {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final int    DRIVERS   = 100;
    private static final int    UPDATES   = 60;

    // Six major metro H3 Res 9 cells (actual H3 indices for real cities)
    // These will coarsen to different Res 3 parents, spreading load.
    private static final double[][] CITY_CENTERS = {
        {37.7749, -122.4194},  // San Francisco
        {34.0522, -118.2437},  // Los Angeles
        {40.7128, -74.0060},   // New York
        {41.8781, -87.6298},   // Chicago
        {29.7604, -95.3698},   // Houston
        {33.4484, -112.0740}   // Phoenix
    };

    public static void main(String[] args) throws Exception {
        TopicAdmin.ensureTopics(BOOTSTRAP);

        var h3     = H3Core.newInstance();
        var mapper = new ObjectMapper();
        var rng    = new Random(42L);

        // Track which partition each send lands on
        var partitionCounts = new ConcurrentHashMap<Integer, AtomicLong>();
        for (int i = 0; i < TopicAdmin.NUM_PARTITIONS; i++) {
            partitionCounts.put(i, new AtomicLong(0));
        }

        var producerProps = buildProducerProps();
        var latch         = new CountDownLatch(DRIVERS * UPDATES);
        var errorCount    = new AtomicLong(0);

        try (var producer = new KafkaProducer<byte[], byte[]>(producerProps)) {

            // Virtual Thread per driver — 100 VTs, zero platform thread overhead
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int d = 0; d < DRIVERS; d++) {
                    final int driverIdx = d;
                    executor.submit(() -> {
                        // Each driver is anchored to a city center, jittered slightly
                        double[] center = CITY_CENTERS[driverIdx % CITY_CENTERS.length];
                        String   driverId = "driver-" + driverIdx;

                        for (int u = 0; u < UPDATES; u++) {
                            // Sub-km jitter within the city
                            double lat = center[0] + (rng.nextDouble() - 0.5) * 0.01;
                            double lng = center[1] + (rng.nextDouble() - 0.5) * 0.01;

                            long h3Res9 = h3.latLngToCell(lat, lng, 9);
                            // Partitioner coarsens this to Res 3 internally
                            byte[] keyBytes = ByteBuffer.allocate(Long.BYTES)
                                                        .putLong(h3Res9)
                                                        .array();

                            var event = new DriverLocationEvent(
                                driverId, lat, lng, h3Res9,
                                System.currentTimeMillis(), "AVAILABLE"
                            );

                            try {
                                byte[] valueBytes = mapper.writeValueAsBytes(event);
                                var record = new ProducerRecord<>(
                                    TopicAdmin.DRIVER_LOCATIONS_TOPIC, keyBytes, valueBytes);

                                producer.send(record, (metadata, ex) -> {
                                    if (ex != null) {
                                        errorCount.incrementAndGet();
                                    } else {
                                        partitionCounts.get(metadata.partition())
                                                       .incrementAndGet();
                                    }
                                    latch.countDown();
                                });
                            } catch (Exception ex) {
                                errorCount.incrementAndGet();
                                latch.countDown();
                            }
                        }
                    });
                }
            } // executor closes here, all VTs complete

            // Wait for all callbacks
            boolean completed = latch.await(30, TimeUnit.SECONDS);
            producer.flush();

            if (!completed) {
                System.err.println("WARN: Timed out waiting for send callbacks");
            }
        }

        printDistributionReport(partitionCounts, errorCount.get());
    }

    private static Properties buildProducerProps() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                  ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  ByteArraySerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                  H3GeoPartitioner.class.getName());

        // Batch tuning: accumulate records for 5ms before flush
        props.put(ProducerConfig.LINGER_MS_CONFIG,    "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,   "65536");  // 64KB
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // Durability: acks=1 for demo; use acks=all in production
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // Bounded retries — avoid masking partitioner bugs with silent retries
        // delivery.timeout.ms must be >= linger.ms + request.timeout.ms
        props.put(ProducerConfig.RETRIES_CONFIG,            "3");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,       "10000");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,"20000");

        return props;
    }

    private static void printDistributionReport(
            Map<Integer, AtomicLong> counts, long errors) {

        System.out.println("\n══════════════════════════════════════════════════");
        System.out.println("  PARTITION DISTRIBUTION REPORT");
        System.out.println("══════════════════════════════════════════════════");

        long total = counts.values().stream().mapToLong(AtomicLong::get).sum();
        double mean = (double) total / TopicAdmin.NUM_PARTITIONS;

        // Only show partitions that received messages
        var active = counts.entrySet().stream()
            .filter(e -> e.getValue().get() > 0)
            .sorted(Map.Entry.comparingByKey())
            .toList();

        System.out.printf("  Total events sent : %,d%n", total);
        System.out.printf("  Errors            : %d%n", errors);
        System.out.printf("  Active partitions : %d / %d%n",
            active.size(), TopicAdmin.NUM_PARTITIONS);
        System.out.printf("  Mean per partition: %.1f%n%n", mean);

        for (var entry : active) {
            int    p    = entry.getKey();
            long   cnt  = entry.getValue().get();
            double pct  = (cnt / mean) * 100;
            String bar  = "█".repeat((int) Math.min(cnt / 10, 50));
            System.out.printf("  P%02d [%4d] %s %.0f%%%n", p, cnt, bar, pct);
        }

        // Coefficient of variation (stddev / mean)
        double sumSq = counts.values().stream()
            .mapToDouble(a -> Math.pow(a.get() - mean, 2)).sum();
        double stddev = Math.sqrt(sumSq / TopicAdmin.NUM_PARTITIONS);
        double cv = (stddev / mean) * 100;

        System.out.println();
        System.out.printf("  Skew CV = %.2f%% (target < 5%% per city group)%n", cv);
        System.out.println("══════════════════════════════════════════════════\n");
    }
}
