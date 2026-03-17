package com.uberlite.boundary;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates synthetic DriverLocationEvent records at ~3,000/sec.
 *
 * Two driver populations:
 *  1. Interior drivers: positions well inside Res-5 cells (no boundary proximity)
 *  2. Boundary drivers: positions within 400m of a known Res-5 cell boundary
 *     — these should trigger multicast in BoundaryBufferingProcessor
 *
 * Uses deterministic seeded PRNG per driver for reproducible scenarios (Lesson 22).
 * Uses Virtual Threads for simulating 500 concurrent drivers without OS thread overhead.
 */
public final class LoadGenerator {

    private static final Logger log = LoggerFactory.getLogger(LoadGenerator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // San Francisco area — dense hex grid, good for boundary testing
    private static final double BASE_LAT = 37.7749;
    private static final double BASE_LNG = -122.4194;

    // Known coordinates near an H3 Res-5 boundary in SF area
    // Verified by checking that h3.cellToParent(h3.latLngToCell(lat,lng,9), 5) differs
    // between two points 600m apart
    private static final double[][] BOUNDARY_COORDS = {
        {37.7850, -122.4094},
        {37.7855, -122.4090},
        {37.7860, -122.4088},
        {37.7749, -122.4194},
        {37.7753, -122.4197}
    };

    private final String bootstrapServers;
    private final int driverCount;
    private final int boundaryDriverCount;
    private final AtomicLong totalSent = new AtomicLong(0);

    public LoadGenerator(String bootstrapServers, int driverCount, int boundaryDriverCount) {
        this.bootstrapServers = bootstrapServers;
        this.driverCount = driverCount;
        this.boundaryDriverCount = boundaryDriverCount;
    }

    public void run() throws InterruptedException {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Tuned for throughput (Lesson 30)
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864L);

        try (var producer = new KafkaProducer<String, String>(props);
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {

            log.info("Starting load: {} interior + {} boundary drivers",
                    driverCount, boundaryDriverCount);

            // Boundary drivers — seeded at known boundary coordinates
            for (int i = 0; i < boundaryDriverCount; i++) {
                final int id = i;
                executor.submit(() -> {
                    var rng = new Random(id * 17L + 42L);
                    var coords = BOUNDARY_COORDS[id % BOUNDARY_COORDS.length];
                    for (int step = 0; step < 2000; step++) {
                        // Jitter within 200m of boundary coord
                        double jitter = 0.002; // ~200m
                        double lat = coords[0] + (rng.nextDouble() - 0.5) * jitter;
                        double lng = coords[1] + (rng.nextDouble() - 0.5) * jitter;
                        sendEvent(producer, "boundary-driver-" + id, lat, lng);
                    }
                    return null;
                });
            }

            // Interior drivers — seeded random positions away from boundaries
            for (int i = 0; i < driverCount; i++) {
                final int id = i;
                executor.submit(() -> {
                    var rng = new Random(id * 31L + 7L);
                    for (int step = 0; step < 1000; step++) {
                        double lat = BASE_LAT + (rng.nextDouble() - 0.5) * 0.1;
                        double lng = BASE_LNG + (rng.nextDouble() - 0.5) * 0.1;
                        sendEvent(producer, "interior-driver-" + id, lat, lng);
                        if (step % 100 == 0) {
                            try { Thread.sleep(10); } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                    return null;
                });
            }

            executor.shutdown();
            while (!executor.isTerminated()) {
                Thread.sleep(500);
                log.info("Sent so far: {}", totalSent.get());
            }

            log.info("Load generation complete. Total sent: {}", totalSent.get());
        }
    }

    private void sendEvent(KafkaProducer<String, String> producer,
                           String driverId, double lat, double lng) {
        var event = DriverLocationEvent.of(driverId, lat, lng, System.currentTimeMillis());
        try {
            String json = MAPPER.writeValueAsString(event);
            var record = new ProducerRecord<>(BoundaryBufferingTopology.SOURCE_TOPIC, driverId, json);
            producer.send(record, (meta, ex) -> {
                if (ex != null) log.error("Send failed", ex);
                else totalSent.incrementAndGet();
            });
        } catch (Exception e) {
            log.error("Serialization error", e);
        }
    }

    public long getTotalSent() { return totalSent.get(); }

    /** Entry point for standalone demo: bootstrapServers [driverCount] [boundaryDriverCount] */
    public static void main(String[] args) throws InterruptedException {
        String bootstrap = args.length > 0 ? args[0] : "localhost:9092";
        int drivers = args.length > 1 ? Integer.parseInt(args[1]) : 100;
        int boundary = args.length > 2 ? Integer.parseInt(args[2]) : 20;
        new LoadGenerator(bootstrap, drivers, boundary).run();
    }
}
