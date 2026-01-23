package com.uberlite;

import com.uber.h3core.H3Core;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Produces 3,000 driver location updates/sec across Manhattan.
 * Uses H3 resolution 7 for geographic partitioning.
 */
public class LocationProducer {
    
    private static final String TOPIC = "driver-locations";
    private static final int TARGET_RATE = 3000; // events/sec
    private static final int NUM_DRIVERS = 500;
    
    // Manhattan bounding box
    private static final double MIN_LAT = 40.7000;
    private static final double MAX_LAT = 40.8800;
    private static final double MIN_LON = -74.0200;
    private static final double MAX_LON = -73.9000;
    
    public static void main(String[] args) throws IOException {
        var h3 = H3Core.newInstance();
        var random = new Random();
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LocationKeySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DriverLocationSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, H3Partitioner.class.getName());
        
        // Durability configuration
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        
        // Metrics
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        
        try (var producer = new KafkaProducer<LocationKey, DriverLocation>(props)) {
            
            System.out.println("Starting producer: " + TARGET_RATE + " events/sec");
            
            var executor = Executors.newVirtualThreadPerTaskExecutor();
            var startTime = System.currentTimeMillis();
            var eventCount = new AtomicLong(0);
            var errorCount = new AtomicLong(0);
            
            // Rate limiter
            var delayNanos = Duration.ofSeconds(1).toNanos() / TARGET_RATE;
            
            executor.submit(() -> {
                long nextSendTime = System.nanoTime();
                
                while (true) {
                    // Generate random location in Manhattan
                    double lat = MIN_LAT + (MAX_LAT - MIN_LAT) * random.nextDouble();
                    double lon = MIN_LON + (MAX_LON - MIN_LON) * random.nextDouble();
                    
                    long h3Index = h3.latLngToCell(lat, lon, 7);
                    String driverId = "D" + random.nextInt(NUM_DRIVERS);
                    
                    var key = new LocationKey(driverId, h3Index);
                    var value = new DriverLocation(
                        driverId,
                        h3Index,
                        lat,
                        lon,
                        System.currentTimeMillis()
                    );
                    
                    producer.send(new ProducerRecord<>(TOPIC, key, value), (metadata, exception) -> {
                        if (exception != null) {
                            errorCount.incrementAndGet();
                            System.err.println("Send failed: " + exception.getMessage());
                        } else {
                            long count = eventCount.incrementAndGet();
                            if (count % 1000 == 0) {
                                long elapsed = System.currentTimeMillis() - startTime;
                                double rate = (count * 1000.0) / elapsed;
                                System.out.printf("Sent %d events (%.1f/sec, errors=%d)%n", 
                                    count, rate, errorCount.get());
                            }
                        }
                    });
                    
                    // Rate limiting
                    nextSendTime += delayNanos;
                    long sleepNanos = nextSendTime - System.nanoTime();
                    if (sleepNanos > 0) {
                        try {
                            Thread.sleep(sleepNanos / 1_000_000, (int) (sleepNanos % 1_000_000));
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }
            });
            
            // Run for 2 minutes
            try {
                Thread.sleep(Duration.ofMinutes(2));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            executor.shutdownNow();
            
            System.out.println("\nFinal stats:");
            System.out.println("  Total events: " + eventCount.get());
            System.out.println("  Errors: " + errorCount.get());
            System.out.println("  Success rate: " + 
                String.format("%.2f%%", 100.0 * (eventCount.get() - errorCount.get()) / eventCount.get()));
        }
    }
}
