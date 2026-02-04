package com.uberlite.batching;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Demonstrates RecordAccumulator batching with Virtual Threads.
 * Sends 3,000 location updates/sec and measures batching efficiency.
 */
public class BatchingProducer {
    private static final String TOPIC = "driver-locations";
    private static final int NUM_DRIVERS = 3000;
    private static final int UPDATES_PER_SECOND = 1;
    private static final int DURATION_SECONDS = 60;
    
    private final KafkaProducer<String, DriverLocationUpdate> producer;
    private final AtomicLong recordsSent = new AtomicLong(0);
    private final AtomicLong sendFailures = new AtomicLong(0);
    private final List<UUID> driverIds;
    
    public BatchingProducer(Properties config) {
        this.producer = new KafkaProducer<>(config);
        this.driverIds = generateDriverIds(NUM_DRIVERS);
    }
    
    private List<UUID> generateDriverIds(int count) {
        var list = new ArrayList<UUID>(count);
        for (int i = 0; i < count; i++) {
            list.add(UUID.randomUUID());
        }
        return list;
    }
    
    public void runLoadTest() throws InterruptedException {
        System.out.println("Starting batching load test:");
        System.out.println("  Drivers: " + NUM_DRIVERS);
        System.out.println("  Updates/sec/driver: " + UPDATES_PER_SECOND);
        System.out.println("  Total throughput: " + (NUM_DRIVERS * UPDATES_PER_SECOND) + " events/sec");
        System.out.println("  Duration: " + DURATION_SECONDS + " seconds");
        System.out.println();
        
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var latch = new CountDownLatch(NUM_DRIVERS);
        
        long startTime = System.currentTimeMillis();
        
        // Start virtual thread per driver
        for (UUID driverId : driverIds) {
            executor.submit(() -> {
                try {
                    sendUpdatesForDriver(driverId, startTime);
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await();
        
        long endTime = System.currentTimeMillis();
        double durationSec = (endTime - startTime) / 1000.0;
        
        producer.flush();
        producer.close();
        
        printStatistics(durationSec);
    }
    
    private void sendUpdatesForDriver(UUID driverId, long startTime) {
        var random = ThreadLocalRandom.current();
        double baseLat = 37.7749 + (random.nextDouble() - 0.5);
        double baseLon = -122.4194 + (random.nextDouble() - 0.5);
        
        for (int i = 0; i < DURATION_SECONDS * UPDATES_PER_SECOND; i++) {
            if (System.currentTimeMillis() - startTime > DURATION_SECONDS * 1000) {
                break;
            }
            
            // Simulate GPS drift
            double lat = baseLat + (random.nextDouble() - 0.5) * 0.002;
            double lon = baseLon + (random.nextDouble() - 0.5) * 0.002;
            
            var update = DriverLocationUpdate.create(driverId, lat, lon);
            var record = new ProducerRecord<>(TOPIC, driverId.toString(), update);
            
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    sendFailures.incrementAndGet();
                } else {
                    recordsSent.incrementAndGet();
                }
            });
            
            // Sleep to maintain update rate
            try {
                Thread.sleep(1000 / UPDATES_PER_SECOND);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    
    private void printStatistics(double durationSec) {
        long sent = recordsSent.get();
        long failed = sendFailures.get();
        
        System.out.println("\n" + "=".repeat(60));
        System.out.println("Batch Statistics (" + String.format("%.1f", durationSec) + "s window):");
        System.out.println("=".repeat(60));
        System.out.println("  Total Records Sent: " + sent);
        System.out.println("  Total Failures: " + failed);
        System.out.println("  Success Rate: " + String.format("%.2f%%", 100.0 * sent / (sent + failed)));
        System.out.println("  Throughput: " + String.format("%.0f", sent / durationSec) + " records/sec");
        System.out.println();
        System.out.println("To view detailed batching metrics:");
        System.out.println("  curl http://localhost:8081/metrics");
        System.out.println("=".repeat(60));
    }
    
    public static void main(String[] args) throws Exception {
        var config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        
        // BATCHING CONFIGURATION - THE CORE OF THIS LESSON
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);  // 32KB batches
        config.put(ProducerConfig.LINGER_MS_CONFIG, 10);      // Wait 10ms to fill batches
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);  // 64MB buffer
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");  // Additional efficiency
        config.put(ProducerConfig.ACKS_CONFIG, "1");  // Leader ack only (fast)
        
        var producer = new BatchingProducer(config);
        producer.runLoadTest();
    }
}
