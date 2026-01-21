package com.uberlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;

/**
 * Produces synthetic driver location updates at ~100 events/sec.
 * Demonstrates proper batching and async send patterns.
 */
public class LogProducer {
    private static final Logger log = LoggerFactory.getLogger(LogProducer.class);
    private static final String TOPIC = "driver-locations";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Random random = new Random();
    
    // San Francisco bounds
    private static final double SF_LAT_MIN = 37.7;
    private static final double SF_LAT_MAX = 37.8;
    private static final double SF_LON_MIN = -122.5;
    private static final double SF_LON_MAX = -122.4;
    
    public static void main(String[] args) throws Exception {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Performance tuning: batch multiple records
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);      // 16KB batches
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);          // Wait 10ms for batch to fill
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.ACKS_CONFIG, "1");              // Leader ack only
        
        try (var producer = new KafkaProducer<String, String>(props)) {
            log.info("Starting producer - sending to topic: {}", TOPIC);
            
            // Use Virtual Threads (Java 21) for concurrent sends
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < 10000; i++) {
                    final int messageIndex = i; // Make effectively final for lambda
                    int driverNum = i % 500; // 500 unique drivers
                    String driverId = "driver-" + driverNum;
                    
                    var location = new DriverLocation(
                        driverId,
                        SF_LAT_MIN + random.nextDouble() * (SF_LAT_MAX - SF_LAT_MIN),
                        SF_LON_MIN + random.nextDouble() * (SF_LON_MAX - SF_LON_MIN),
                        System.currentTimeMillis(),
                        random.nextBoolean()
                    );
                    
                    String json = mapper.writeValueAsString(location);
                    var record = new ProducerRecord<>(TOPIC, driverId, json);
                    
                    // Async send with callback
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            log.error("Send failed for driver: {}", driverId, exception);
                        } else if (messageIndex % 1000 == 0) {
                            log.info("Sent offset {} to partition {}", 
                                metadata.offset(), metadata.partition());
                        }
                    });
                    
                    // Rate limiting: ~100 msg/sec
                    if (i % 100 == 0) {
                        Thread.sleep(1000);
                    }
                }
                
                log.info("Flushing final batch...");
                producer.flush(); // Block until all async sends complete
                log.info("Producer completed successfully");
            }
        }
    }
}
