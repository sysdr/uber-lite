package com.uberlite.async;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * ANTI-PATTERN: Synchronous producer for comparison.
 * This will achieve ~200 events/sec maximum.
 */
public class SyncProducerDemo {
    private static final String TOPIC = "driver-locations";
    
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = createProducer();
        var startTime = System.nanoTime();
        var sentCount = 0;
        
        System.out.println("Starting SYNCHRONOUS send test (will be slow)...\n");
        
        for (int i = 0; i < 1000; i++) {
            var location = DriverLocation.random("driver-" + i);
            var record = new ProducerRecord<>(TOPIC, "driver-" + i, location);
            
            // BLOCKING CALL - waits for ACK
            producer.send(record).get();
            sentCount++;
            
            if (sentCount % 100 == 0) {
                var elapsedMs = (System.nanoTime() - startTime) / 1_000_000;
                var throughput = (sentCount * 1000.0) / elapsedMs;
                System.out.printf("Sent %d events | Throughput: %.0f events/sec\n", 
                    sentCount, throughput);
            }
        }
        
        var totalMs = (System.nanoTime() - startTime) / 1_000_000;
        var avgThroughput = (sentCount * 1000.0) / totalMs;
        
        System.out.printf("""
            
            SYNCHRONOUS RESULTS:
            Total: %d events
            Duration: %,d ms
            Throughput: %.0f events/sec
            
            ❌ Compare to async: ~5,000 events/sec
            """, sentCount, totalMs, avgThroughput);
        
        producer.close(Duration.ofSeconds(2));
    }
    
    private static KafkaProducer<String, DriverLocation> createProducer() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        
        return new KafkaProducer<>(props);
    }
}
