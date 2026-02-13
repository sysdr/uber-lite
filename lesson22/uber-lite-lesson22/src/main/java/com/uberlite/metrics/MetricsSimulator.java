package com.uberlite.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.h3core.H3Core;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.sun.net.httpserver.HttpServer;

public class MetricsSimulator {
    private static final int NUM_DRIVERS = 1000;
    private static final String TOPIC = "driver-locations";
    private static final ObjectMapper mapper = new ObjectMapper();
    
    public static void main(String[] args) throws Exception {
        // Register custom MBean
        SpatialMetrics spatialMetrics = new SpatialMetrics();
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName("com.uberlite:type=SpatialMetrics");
        mbs.registerMBean(spatialMetrics, name);
        
        System.out.println("JMX Server started on port 9999");
        System.out.println("Connect with: jconsole localhost:9999");
        System.out.println("Custom metrics at: com.uberlite:type=SpatialMetrics");
        
        // Start HTTP metrics server on port 8082 for dashboard
        HttpServer metricsServer = HttpServer.create(new InetSocketAddress(8082), 0);
        metricsServer.createContext("/api/metrics", exchange -> {
            String json = String.format(
                "{\"res9Count\":%d,\"res10Count\":%d,\"totalLocations\":%d,\"averageH3Resolution\":%.2f}",
                spatialMetrics.getRes9Count(), spatialMetrics.getRes10Count(),
                spatialMetrics.getTotalLocations(), spatialMetrics.getAverageH3Resolution());
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
        metricsServer.setExecutor(null);
        metricsServer.start();
        System.out.println("Metrics API: http://localhost:8082/api/metrics");
        
        // Create Kafka producer with optimized settings
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432"); // 32MB
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        H3Core h3 = H3Core.newInstance();
        Random random = new Random();
        
        // San Francisco bounding box
        double minLat = 37.7, maxLat = 37.8;
        double minLon = -122.5, maxLon = -122.4;
        
        // Simulate drivers using virtual threads
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        
        for (int i = 0; i < NUM_DRIVERS; i++) {
            final int driverId = i;
            executor.submit(() -> {
                try {
                    while (true) {
                        double lat = minLat + (maxLat - minLat) * random.nextDouble();
                        double lon = minLon + (maxLon - minLon) * random.nextDouble();
                        
                        // Randomly choose resolution 9 or 10
                        int resolution = random.nextBoolean() ? 9 : 10;
                        long h3Index = h3.latLngToCell(lat, lon, resolution);
                        String hexId = Long.toHexString(h3Index);
                        
                        DriverLocation location = new DriverLocation(
                            "driver-" + driverId,
                            lat,
                            lon,
                            System.currentTimeMillis(),
                            hexId
                        );
                        
                        String json = mapper.writeValueAsString(location);
                        ProducerRecord<String, String> record = new ProducerRecord<>(
                            TOPIC,
                            location.driverId(),
                            json
                        );
                        
                        producer.send(record);
                        spatialMetrics.recordResolution(resolution);
                        
                        // Update every 2 seconds
                        Thread.sleep(2000);
                    }
                } catch (Exception e) {
                    System.err.println("Driver " + driverId + " error: " + e.getMessage());
                }
            });
        }
        
        // Keep main thread alive
        System.out.println("Simulating " + NUM_DRIVERS + " drivers...");
        System.out.println("Expected throughput: ~" + (NUM_DRIVERS / 2) + " events/sec");
        
        // Print stats every 10 seconds
        while (true) {
            Thread.sleep(10000);
            System.out.printf("Custom metrics - Total: %d, Res9: %d, Res10: %d, Avg Resolution: %.2f%n",
                spatialMetrics.getTotalLocations(),
                spatialMetrics.getRes9Count(),
                spatialMetrics.getRes10Count(),
                spatialMetrics.getAverageH3Resolution()
            );
        }
    }
}
