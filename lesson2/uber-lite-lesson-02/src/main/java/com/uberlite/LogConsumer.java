package com.uberlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumer demonstrating proper offset management patterns.
 * Key lessons:
 * 1. Batched async commits reduce fsync overhead
 * 2. Sync commit on shutdown guarantees no data loss
 * 3. Offset tracking at message-level granularity
 */
public class LogConsumer {
    private static final Logger log = LoggerFactory.getLogger(LogConsumer.class);
    private static final String TOPIC = "driver-locations";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final AtomicBoolean running = new AtomicBoolean(true);
    
    public static void main(String[] args) throws Exception {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "location-processor-v1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // Disable auto-commit - we manage offsets manually
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(TOPIC));
            
            // Graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received");
                running.set(false);
            }));
            
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            long lastCommitTime = System.currentTimeMillis();
            int messagesProcessed = 0;
            
            log.info("Starting consumer - subscribed to: {}", TOPIC);
            
            while (running.get()) {
                var records = consumer.poll(Duration.ofSeconds(1));
                
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        var location = mapper.readValue(record.value(), DriverLocation.class);
                        
                        // Simulate processing
                        processLocation(location);
                        
                        // Track offset for this partition
                        var partition = new TopicPartition(record.topic(), record.partition());
                        offsetsToCommit.put(partition, 
                            new OffsetAndMetadata(record.offset() + 1)); // Next offset to read
                        
                        messagesProcessed++;
                        
                    } catch (Exception e) {
                        log.error("Failed to process record at offset {}", record.offset(), e);
                        // In production: send to DLQ topic
                    }
                }
                
                // Commit strategy: every 5000 messages OR every 10 seconds
                long now = System.currentTimeMillis();
                if (messagesProcessed >= 5000 || (now - lastCommitTime) > 10_000) {
                    if (!offsetsToCommit.isEmpty()) {
                        consumer.commitAsync(offsetsToCommit, (offsets, exception) -> {
                            if (exception != null) {
                                log.error("Async commit failed", exception);
                            } else {
                                log.info("Committed offsets: {}", offsets);
                            }
                        });
                        offsetsToCommit.clear();
                        lastCommitTime = now;
                        messagesProcessed = 0;
                    }
                }
            }
            
            // Final synchronous commit before shutdown
            if (!offsetsToCommit.isEmpty()) {
                log.info("Performing final sync commit...");
                consumer.commitSync(offsetsToCommit);
                log.info("Final offsets committed: {}", offsetsToCommit);
            }
            
            log.info("Consumer shutdown complete");
        }
    }
    
    private static void processLocation(DriverLocation location) {
        // Placeholder for actual processing logic
        // In real system: update spatial index, check for matches, etc.
        if (log.isDebugEnabled()) {
            log.debug("Processed driver {} at ({}, {})", 
                location.driverId(), location.latitude(), location.longitude());
        }
    }
}
