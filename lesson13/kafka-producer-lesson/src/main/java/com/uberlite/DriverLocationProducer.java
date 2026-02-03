package com.uberlite;

import com.uber.h3core.H3Core;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * High-throughput driver location producer using:
 * - H3 resolution 9 for spatial partitioning (~0.1km² cells)
 * - Batching with linger.ms=5 for 200x throughput vs sync sends
 * - Binary serialization (40 bytes vs 80+ for JSON)
 * - Virtual threads for 10k+ concurrent async callbacks
 * - LZ4 compression for 3-5x size reduction
 */
public class DriverLocationProducer {
    private static final Logger log = LoggerFactory.getLogger(DriverLocationProducer.class);
    private static final String TOPIC = "driver-locations";
    private static final int NUM_DRIVERS = 10_000;
    private static final int EVENTS_PER_DRIVER = 1;
    
    public static void main(String[] args) throws Exception {
        var h3 = H3Core.newInstance();
        var metrics = new MetricsCollector();
        
        // Producer configuration optimized for high-velocity spatial data
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        
        // Batching configuration
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768); // 32KB batches
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5); // Wait 5ms to accumulate
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        
        // Memory management
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864); // 64MB
        
        // Reliability (exactly-once semantics)
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        // Custom H3-based partitioner
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, H3Partitioner.class);
        
        log.info("Starting producer with batching enabled (linger.ms=5, batch.size=32KB)");
        
        try (var producer = new KafkaProducer<Long, byte[]>(props);
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            
            long startTime = System.currentTimeMillis();
            
            // Generate and send driver location updates
            for (int i = 0; i < NUM_DRIVERS; i++) {
                for (int j = 0; j < EVENTS_PER_DRIVER; j++) {
                    var location = DriverLocationUpdate.random(i);
                    
                    // Convert lat/lon to H3 index at resolution 9
                    long h3Index = h3.latLngToCell(
                        location.lat(), 
                        location.lon(), 
                        9 // ~0.1km² hexagons
                    );
                    
                    var record = new ProducerRecord<>(
                        TOPIC,
                        h3Index, // Key: H3 cell for partitioning
                        location.toBytes() // Value: binary-encoded location
                    );
                    
                    // Async send with virtual thread callback
                    producer.send(record, (metadata, exception) -> {
                        executor.submit(() -> {
                            if (exception != null) {
                                log.error("Send failed for driver {}", location.driverId(), exception);
                                metrics.recordError();
                            } else {
                                long latency = System.currentTimeMillis() - location.timestampMs();
                                metrics.recordSuccess(metadata.partition(), latency);
                                
                                if (metrics.getPartitionDistribution().values().stream()
                                    .mapToLong(Long::longValue).sum() % 1000 == 0) {
                                    log.info("Sent {} records, partition CV: {}", 
                                        metadata.offset(), 
                                        String.format("%.3f", metrics.getPartitionCV()));
                                }
                            }
                        });
                    });
                }
                
                // Rate limiting to ~3000 events/sec
                if (i % 100 == 0) {
                    Thread.sleep(30);
                }
            }
            
            // Flush remaining batches
            log.info("Flushing producer...");
            producer.flush();
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            log.info("=== PRODUCER METRICS ===");
            log.info(metrics.toJson());
            log.info("========================");
            
            // Write metrics to file for verification script
            java.nio.file.Files.writeString(
                java.nio.file.Path.of("metrics.json"),
                metrics.toJson()
            );
            
            // Wait for callbacks to complete
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }
}
