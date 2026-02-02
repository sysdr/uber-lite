package com.uberlite.topics;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Validates topic engineering decisions by:
 * 1. Verifying partition counts and replication factors
 * 2. Producing test load (1000 messages)
 * 3. Measuring partition balance (skew should be < 10%)
 */
public class TopicValidator {
    
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "driver-locations";
    private static final int EXPECTED_PARTITIONS = 60;
    private static final int EXPECTED_REPLICATION_FACTOR = 3;
    private static final int TEST_MESSAGE_COUNT = 1000;
    
    record DriverLocation(String driverId, double lat, double lon, long timestamp) {}
    
    public static void main(String[] args) {
        System.out.println("🔍 Validating Topic Engineering Configuration\n");
        
        var validator = new TopicValidator();
        
        try {
            // Step 1: Validate topic metadata
            validator.validateTopicMetadata();
            
            // Step 2: Produce test load
            var partitionCounts = validator.produceTestLoad();
            
            // Step 3: Analyze partition balance
            validator.analyzePartitionBalance(partitionCounts);
            
            System.out.println("\n✅ PASS: Topic engineering validated");
            System.exit(0);
            
        } catch (Exception e) {
            System.err.println("\n❌ FAIL: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private void validateTopicMetadata() throws ExecutionException, InterruptedException {
        Properties adminProps = new Properties();
        adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        
        try (var admin = AdminClient.create(adminProps)) {
            var topics = Set.of(TOPIC, "rider-requests", "ride-matches");
            var descriptions = admin.describeTopics(topics).allTopicNames().get();

            var driverTopic = descriptions.get(TOPIC);
            if (driverTopic == null) throw new RuntimeException("Topic not found: " + TOPIC);
            validateTopic(driverTopic, EXPECTED_PARTITIONS, EXPECTED_REPLICATION_FACTOR);

            var riderTopic = descriptions.get("rider-requests");
            if (riderTopic == null) throw new RuntimeException("Topic not found: rider-requests");
            validateTopic(riderTopic, 12, EXPECTED_REPLICATION_FACTOR);

            var rideMatchesTopic = descriptions.get("ride-matches");
            if (rideMatchesTopic == null) throw new RuntimeException("Topic not found: ride-matches");
            validateTopic(rideMatchesTopic, 12, EXPECTED_REPLICATION_FACTOR);
        }
    }
    
    private void validateTopic(TopicDescription topic, int expectedPartitions, int expectedRF) {
        int actualPartitions = topic.partitions().size();
        int actualRF = topic.partitions().get(0).replicas().size();
        
        System.out.printf("✓ Topic %s: %d partitions, RF=%d%n", 
            topic.name(), actualPartitions, actualRF);
        
        if (actualPartitions != expectedPartitions) {
            throw new RuntimeException(String.format(
                "Topic %s has %d partitions, expected %d", 
                topic.name(), actualPartitions, expectedPartitions));
        }
        
        if (actualRF != expectedRF) {
            throw new RuntimeException(String.format(
                "Topic %s has RF=%d, expected %d", 
                topic.name(), actualRF, expectedRF));
        }
        
        // Check ISR health
        for (var partition : topic.partitions()) {
            int isrSize = partition.isr().size();
            if (isrSize != expectedRF) {
                throw new RuntimeException(String.format(
                    "Partition %d has ISR size %d, expected %d (under-replicated)", 
                    partition.partition(), isrSize, expectedRF));
            }
        }
        
        System.out.printf("✓ All partitions have ISR count = %d%n", expectedRF);
    }
    
    private Map<Integer, Integer> produceTestLoad() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put("request.timeout.ms", "10000");
        props.put("max.block.ms", "10000");

        Map<Integer, Integer> partitionCounts = new HashMap<>();

        System.out.printf("%n📤 Producing %d test messages...%n", TEST_MESSAGE_COUNT);

        try (var producer = new KafkaProducer<String, String>(props)) {
            for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
                String driverId = "driver_" + i;
                
                // Simulate NYC location
                double lat = 40.7128 + (Math.random() - 0.5) * 0.1;
                double lon = -74.0060 + (Math.random() - 0.5) * 0.1;
                long timestamp = System.currentTimeMillis();
                
                String value = String.format(
                    "{\"driverId\":\"%s\",\"lat\":%.6f,\"lon\":%.6f,\"timestamp\":%d}",
                    driverId, lat, lon, timestamp
                );
                
                var record = new ProducerRecord<>(TOPIC, driverId, value);
                
                var metadata = producer.send(record).get();
                partitionCounts.merge(metadata.partition(), 1, Integer::sum);
            }
            
            producer.flush();
        } catch (Exception e) {
            throw new RuntimeException("Failed to produce test load", e);
        }
        
        System.out.printf("✓ Produced %d messages across %d partitions%n", 
            TEST_MESSAGE_COUNT, partitionCounts.size());
        
        return partitionCounts;
    }
    
    private void analyzePartitionBalance(Map<Integer, Integer> partitionCounts) {
        int totalMessages = partitionCounts.values().stream().mapToInt(Integer::intValue).sum();
        double expectedPerPartition = (double) totalMessages / EXPECTED_PARTITIONS;
        
        int maxCount = partitionCounts.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        int minCount = partitionCounts.values().stream().mapToInt(Integer::intValue).min().orElse(0);
        
        double maxSkew = Math.abs((maxCount - expectedPerPartition) / expectedPerPartition) * 100;
        double minSkew = Math.abs((minCount - expectedPerPartition) / expectedPerPartition) * 100;
        double overallSkew = Math.max(maxSkew, minSkew);
        
        System.out.printf("%n📊 Partition Balance Analysis:%n");
        System.out.printf("  Expected per partition: %.1f messages%n", expectedPerPartition);
        System.out.printf("  Max partition count: %d (%.1f%% skew)%n", maxCount, maxSkew);
        System.out.printf("  Min partition count: %d (%.1f%% skew)%n", minCount, minSkew);
        System.out.printf("  Overall skew: %.1f%%%n", overallSkew);
        
        if (overallSkew > 10.0) {
            throw new RuntimeException(String.format(
                "Partition skew %.1f%% exceeds threshold of 10%%", overallSkew));
        }
        
        System.out.printf("✓ Partition balance: max skew %.1f%% (threshold: 10%%)%n", overallSkew);
    }
}
