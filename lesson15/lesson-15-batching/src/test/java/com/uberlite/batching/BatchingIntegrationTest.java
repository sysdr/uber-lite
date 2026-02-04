package com.uberlite.batching;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;
import java.util.*;

public class BatchingIntegrationTest {
    private static KafkaContainer kafka;
    
    @BeforeAll
    static void setup() {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));
        kafka.start();
    }
    
    @AfterAll
    static void teardown() {
        kafka.stop();
    }
    
    @Test
    void testBatchingEfficiency() throws Exception {
        var producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        
        try (var producer = new KafkaProducer<String, DriverLocationUpdate>(producerConfig)) {
            // Send 1000 updates
            for (int i = 0; i < 1000; i++) {
                var driverId = UUID.randomUUID();
                var update = DriverLocationUpdate.create(driverId, 37.7749, -122.4194);
                producer.send(new ProducerRecord<>("test-topic", driverId.toString(), update));
            }
            producer.flush();
            
            // Check metrics
            var metrics = producer.metrics();
            var batchSizeMetric = metrics.values().stream()
                .filter(m -> m.metricName().name().equals("batch-size-avg"))
                .findFirst()
                .orElseThrow();
            
            double avgBatchSize = (double) batchSizeMetric.metricValue();
            System.out.println("Average batch size: " + avgBatchSize + " bytes");
            
            // With linger.ms=10 and 1000 records, batches should be well-formed
            Assertions.assertTrue(avgBatchSize > 1000, "Batching should aggregate multiple records");
        }
    }
}
