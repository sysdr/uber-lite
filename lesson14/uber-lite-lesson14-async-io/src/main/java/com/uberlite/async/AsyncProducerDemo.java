package com.uberlite.async;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AsyncProducerDemo {
    private static final Logger logger = LoggerFactory.getLogger(AsyncProducerDemo.class);
    private static final String TOPIC = "driver-locations";
    private static final int NUM_DRIVERS = 1000;
    private static final Duration UPDATE_INTERVAL = Duration.ofSeconds(2);
    
    public static void main(String[] args) throws InterruptedException {
        System.out.println("""
            
            ╔══════════════════════════════════════════════════════════════════╗
            ║  Lesson 14: Asynchronous I/O - High-Throughput Producer Demo    ║
            ╚══════════════════════════════════════════════════════════════════╝
            
            Starting simulation with:
            - Drivers: %,d
            - Update Interval: %d seconds
            - Target Rate: %,d events/sec
            
            """.formatted(NUM_DRIVERS, UPDATE_INTERVAL.getSeconds(), 
                          NUM_DRIVERS / UPDATE_INTERVAL.getSeconds()));
        
        var metrics = new ProducerMetrics();
        var producer = createOptimizedProducer();
        
        // Graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(Thread.ofVirtual().unstarted(() -> {
            logger.info("Shutting down producer...");
            producer.close(Duration.ofSeconds(5));
            metrics.printStats();
        }));
        
        // Run comparison
        runAsyncSimulation(producer, metrics);
    }
    
    private static KafkaProducer<String, DriverLocation> createOptimizedProducer() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        
        // CRITICAL: Async I/O optimizations
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");           // Wait 10ms for batching
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");       // 32KB batches
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");   // Fast compression
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864"); // 64MB buffer
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");      // Fail fast if buffer full
        
        // Retries (idempotence requires acks=all; for throughput we use acks=1)
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        
        return new KafkaProducer<>(props);
    }
    
    private static void runAsyncSimulation(
            KafkaProducer<String, DriverLocation> producer,
            ProducerMetrics metrics) throws InterruptedException {
        
        var startTime = System.currentTimeMillis();
        
        // Virtual thread per driver - 1000 threads with negligible overhead
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < NUM_DRIVERS; i++) {
                final String driverId = "driver-" + i;
                
                executor.submit(() -> {
                    while (true) {
                        try {
                            var location = DriverLocation.random(driverId);
                            var sendTimestamp = System.currentTimeMillis();
                            
                            // ASYNC SEND with Callback
                            producer.send(
                                new ProducerRecord<>(TOPIC, driverId, location),
                                createCallback(driverId, sendTimestamp, metrics)
                            );
                            
                            Thread.sleep(UPDATE_INTERVAL);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        } catch (Exception e) {
                            logger.error("Error in driver simulation: {}", driverId, e);
                        }
                    }
                });
            }
            
            // Run for 30 seconds
            Thread.sleep(30_000);
        }
        
        var elapsedSec = (System.currentTimeMillis() - startTime) / 1000.0;
        var throughput = metrics.getSuccessCount() / elapsedSec;
        
        System.out.printf("""
            
            ✅ ASYNC SIMULATION COMPLETE
            Duration: %.1f seconds
            Throughput: %,.0f events/sec
            
            """, elapsedSec, throughput);
    }
    
    private static Callback createCallback(
            String driverId, 
            long sendTimestamp, 
            ProducerMetrics metrics) {
        
        return (metadata, exception) -> {
            if (exception != null) {
                // Handle failure - log and increment counter
                logger.error("Failed to send location for {}: {}", 
                    driverId, exception.getMessage());
                metrics.recordFailure();
                
                // In production: send to DLQ, trigger alert, etc.
            } else {
                // Success - record metrics
                metrics.recordSuccess(sendTimestamp);
                
                if (metrics.getSuccessCount() % 1000 == 0) {
                    logger.info("Sent {} events | Partition: {} | Offset: {} | Latency: {}ms",
                        metrics.getSuccessCount(),
                        metadata.partition(),
                        metadata.offset(),
                        String.format("%.2f", metrics.getAverageLatency())
                    );
                }
            }
        };
    }
}
