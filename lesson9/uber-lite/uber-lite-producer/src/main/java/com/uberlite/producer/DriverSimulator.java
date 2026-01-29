package com.uberlite.producer;

import com.uberlite.common.DriverLocation;
import com.uberlite.common.JsonSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Simulates high-velocity driver location updates (3,000+ events/sec).
 * Uses Virtual Threads to achieve concurrency without thread pool overhead.
 * 
 * Configuration mirrors Uber's production settings:
 * - Large batches (1MB) to reduce network RPCs
 * - 10ms linger to fill batches at 3k/sec rate
 * - LZ4 compression (CPU-efficient, 3x compression ratio)
 * - max.in.flight=1 for ordering guarantee
 */
public class DriverSimulator {
    private static final Logger log = LoggerFactory.getLogger(DriverSimulator.class);
    private static final String TOPIC = "driver-locations";
    
    // NYC bounding box
    private static final double NYC_LAT_MIN = 40.477399;
    private static final double NYC_LAT_MAX = 40.917577;
    private static final double NYC_LON_MIN = -74.259090;
    private static final double NYC_LON_MAX = -73.700272;

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        int targetRps = Integer.parseInt(System.getenv().getOrDefault("TARGET_RPS", "3000"));
        
        var producer = createProducer(bootstrapServers);
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var random = new Random();

        log.info("Starting driver simulator: {} events/sec", targetRps);
        
        long startTime = System.currentTimeMillis();
        long messagesSent = 0;

        while (true) {
            executor.submit(() -> {
                String driverId = "driver-" + random.nextInt(10000);
                double lat = NYC_LAT_MIN + (NYC_LAT_MAX - NYC_LAT_MIN) * random.nextDouble();
                double lon = NYC_LON_MIN + (NYC_LON_MAX - NYC_LON_MIN) * random.nextDouble();
                
                var location = DriverLocation.of(driverId, lat, lon);
                var record = new ProducerRecord<>(TOPIC, location.driverId(), location);
                
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Failed to send: {}", location, exception);
                    }
                });
            });

            messagesSent++;
            
            // Rate limiting: sleep to achieve target RPS
            if (messagesSent % 100 == 0) {
                long elapsed = System.currentTimeMillis() - startTime;
                long expected = (messagesSent * 1000L) / targetRps;
                long sleepMs = expected - elapsed;
                if (sleepMs > 0) {
                    try {
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                
                if (messagesSent % 1000 == 0) {
                    log.info("Sent {} messages, rate: {} msg/sec", 
                        messagesSent, 1000.0 * messagesSent / elapsed);
                }
            }
        }
    }

    private static KafkaProducer<String, DriverLocation> createProducer(String bootstrapServers) {
        var props = new Properties();
        
        // Connection
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "driver-simulator");
        
        // Durability: Wait for all in-sync replicas
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        // Batching: Critical for high throughput
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1048576);      // 1MB (vs 16KB default)
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);            // 10ms wait (vs 0ms default)
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);  // 64MB buffer
        
        // Compression: LZ4 is CPU-efficient
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        
        // Ordering: max.in.flight=1 guarantees order within partition
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        
        // Timeouts
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        
        log.info("Producer config: batch.size=1MB, linger.ms=10, acks=all");
        
        return new KafkaProducer<>(
            props,
            new StringSerializer(),
            new JsonSerde.JsonSerializer<>()
        );
    }
}
