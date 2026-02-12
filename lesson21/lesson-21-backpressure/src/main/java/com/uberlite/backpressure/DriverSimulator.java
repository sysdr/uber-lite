package com.uberlite.backpressure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class DriverSimulator {
    private static final Logger LOG = LoggerFactory.getLogger(DriverSimulator.class);
    private static final int DRIVER_COUNT = 10_000;
    private static final String TOPIC = "driver-locations";

    public static void main(String[] args) throws Exception {
        boolean backpressureEnabled = !Boolean.parseBoolean(
            System.getProperty("backpressure.enabled", "true").equals("false") ? "true" : "false"
        );
        
        LOG.info("Starting Driver Simulator: {} drivers, backpressure={}", 
            DRIVER_COUNT, backpressureEnabled);

        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        
        // Fail-fast configuration
        props.put("buffer.memory", "16777216"); // 16MB
        props.put("max.block.ms", "0"); // Don't block, throw exception
        props.put("linger.ms", "5"); // Batch for 5ms to improve throughput
        props.put("batch.size", "32768"); // 32KB batches
        props.put("compression.type", "lz4");
        props.put("acks", "1"); // Faster acknowledgments

        var producer = new BackpressureProducer(props, backpressureEnabled);
        var running = new AtomicBoolean(true);

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down simulator...");
            running.set(false);
        }));

        // Generate initial driver fleet
        List<SimulatedDriver> drivers = new ArrayList<>();
        for (int i = 0; i < DRIVER_COUNT; i++) {
            drivers.add(new SimulatedDriver("driver-" + i));
        }

        // Start metrics reporter
        var metricsThread = Thread.startVirtualThread(() -> {
            while (running.get()) {
                try {
                    Thread.sleep(5000);
                    LOG.info("Metrics: sent={}, buffer_exhaustion={}, buffer_util={}%",
                        producer.getSuccessCount(),
                        producer.getBufferExhaustionCount(),
                        String.format("%.1f", producer.getBufferUtilization()));
                } catch (InterruptedException e) {
                    break;
                }
            }
        });

        // Simulation loop using virtual threads
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            while (running.get()) {
                long cycleStart = System.currentTimeMillis();
                
                for (var driver : drivers) {
                    executor.submit(() -> {
                        try {
                            var location = driver.nextLocation();
                            producer.sendWithBackpressure(TOPIC, location);
                        } catch (Exception e) {
                            LOG.error("Failed to send location for {}: {}", 
                                driver.id(), e.getMessage());
                        }
                    });
                }

                // Target 1 update per second per driver
                long elapsed = System.currentTimeMillis() - cycleStart;
                long sleepTime = Math.max(0, 1000 - elapsed);
                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                }
            }
        } finally {
            producer.close();
            LOG.info("Simulator stopped. Total sent: {}, exhaustion events: {}",
                producer.getSuccessCount(), producer.getBufferExhaustionCount());
        }
    }

    static class SimulatedDriver {
        private final String id;
        private double latitude;
        private double longitude;

        SimulatedDriver(String id) {
            this.id = id;
            // Random starting position in San Francisco
            this.latitude = 37.7749 + (ThreadLocalRandom.current().nextDouble() - 0.5) * 0.1;
            this.longitude = -122.4194 + (ThreadLocalRandom.current().nextDouble() - 0.5) * 0.1;
        }

        String id() {
            return id;
        }

        DriverLocation nextLocation() {
            // Random walk: move 0.0001 degrees (~10 meters)
            latitude += (ThreadLocalRandom.current().nextDouble() - 0.5) * 0.0001;
            longitude += (ThreadLocalRandom.current().nextDouble() - 0.5) * 0.0001;
            
            return new DriverLocation(
                id,
                latitude,
                longitude,
                System.currentTimeMillis(),
                "ACTIVE"
            );
        }
    }
}
