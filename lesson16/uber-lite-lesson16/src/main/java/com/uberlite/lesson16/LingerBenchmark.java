package com.uberlite.lesson16;

import com.uber.h3core.H3Core;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class LingerBenchmark {
    private static final String TOPIC = "driver-locations";
    private static final int NUM_EVENTS = 3000;
    private static final H3Core h3;
    
    static {
        try {
            h3 = H3Core.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "baseline";
        
        System.out.println("🔥 Lesson 16: linger.ms Benchmark");
        System.out.println("Mode: " + mode);
        System.out.println("Target: " + NUM_EVENTS + " events\n");
        
        switch (mode) {
            case "baseline" -> runBenchmark(0, "BASELINE (linger.ms=0)");
            case "optimized" -> runBenchmark(10, "OPTIMIZED (linger.ms=10)");
            case "extreme" -> runBenchmark(50, "EXTREME (linger.ms=50)");
            default -> {
                System.out.println("Usage: java -jar lesson16.jar [baseline|optimized|extreme]");
                System.exit(1);
            }
        }
    }
    
    private static void runBenchmark(int lingerMs, String label) throws Exception {
        Properties props = createProducerConfig(lingerMs);
        
        try (var producer = new KafkaProducer<String, DriverLocationUpdate>(props)) {
            long startTime = System.currentTimeMillis();
            
            // Simulate high-velocity GPS stream
            for (int i = 0; i < NUM_EVENTS; i++) {
                String driverId = "driver-" + (i % 100); // 100 unique drivers
                
                // San Francisco coordinates with slight jitter
                double lat = 37.7749 + (Math.random() - 0.5) * 0.1;
                double lon = -122.4194 + (Math.random() - 0.5) * 0.1;
                long h3Index = h3.latLngToCell(lat, lon, 9);
                
                var update = DriverLocationUpdate.create(driverId, lat, lon, h3Index);
                var record = new ProducerRecord<>(TOPIC, driverId, update);
                
                producer.send(record);
                
                // Simulate realistic GPS update interval (1ms average)
                if (i % 100 == 0) {
                    Thread.sleep(1);
                }
            }
            
            // Force final flush
            producer.flush();
            
            long duration = System.currentTimeMillis() - startTime;
            
            // Wait for metrics to stabilize
            Thread.sleep(500);
            
            var metrics = ProducerMetrics.capture(producer);
            
            System.out.println("\n" + "=".repeat(60));
            System.out.println(label);
            System.out.println("=".repeat(60));
            System.out.printf("Duration:           %d ms%n", duration);
            System.out.printf("Throughput:         %.0f events/sec%n", 
                (NUM_EVENTS * 1000.0) / duration);
            
            metrics.print("Producer Metrics");
            
            // Calculate efficiency
            double batchEfficiency = (metrics.batchSizeAvg() / 16384.0) * 100;
            double requestReduction = (1 - (metrics.requestRate() / NUM_EVENTS)) * 100;
            
            System.out.printf("""
                
                === Efficiency Analysis ===
                Batch Fill Ratio:   %.1f%% (target: >80%%)
                Request Reduction:  %.1f%% (target: >95%%)
                
                """, batchEfficiency, requestReduction);
        }
    }
    
    private static Properties createProducerConfig(int lingerMs) {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        
        // Core batching configuration
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);  // 16KB
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        
        // Performance tuning
        props.put(ProducerConfig.ACKS_CONFIG, "1");  // Leader-only for latency
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);  // 32MB
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        
        // Metrics
        props.put(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 5000);
        
        return props;
    }
}
