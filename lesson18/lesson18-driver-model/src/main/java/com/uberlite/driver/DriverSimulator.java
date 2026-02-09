package com.uberlite.driver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import com.uberlite.driver.model.Driver;
import com.uberlite.driver.producer.H3CellPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DriverSimulator {
    private static final String TOPIC = "driver-locations";
    private static final int NUM_DRIVERS = 1000;
    private static final int UPDATE_INTERVAL_SEC = 2;
    
    // NYC boundaries (Manhattan area)
    private static final double NYC_LAT_MIN = 40.70;
    private static final double NYC_LAT_MAX = 40.85;
    private static final double NYC_LNG_MIN = -74.02;
    private static final double NYC_LNG_MAX = -73.90;

    public static void main(String[] args) throws Exception {
        var h3 = H3Core.newInstance();
        var mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        var random = new Random();

        // Initialize drivers with random positions in NYC
        List<Driver> drivers = new ArrayList<>();
        Set<Long> uniqueCells = new HashSet<>();
        
        for (int i = 0; i < NUM_DRIVERS; i++) {
            double lat = NYC_LAT_MIN + random.nextDouble() * (NYC_LAT_MAX - NYC_LAT_MIN);
            double lng = NYC_LNG_MIN + random.nextDouble() * (NYC_LNG_MAX - NYC_LNG_MIN);
            long h3Cell = h3.latLngToCell(lat, lng, 9);
            uniqueCells.add(h3Cell);
            
            int heading = random.nextInt(360);
            double speedMps = 5.0 + random.nextDouble() * 10.0; // 5-15 m/s (18-54 km/h)
            
            drivers.add(new Driver(
                "driver-" + i,
                h3Cell,
                heading,
                speedMps,
                Instant.now()
            ));
        }
        
        System.out.println("Initialized " + NUM_DRIVERS + " drivers across " + 
                          uniqueCells.size() + " H3 cells");

        // Kafka producer with H3 partitioner
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, H3CellPartitioner.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");

        var producer = new KafkaProducer<String, String>(props);

        // Virtual thread executor for async updates
        var executor = Executors.newScheduledThreadPool(
            Runtime.getRuntime().availableProcessors(),
            Thread.ofVirtual().factory()
        );

        // Schedule driver position updates
        executor.scheduleAtFixedRate(() -> {
            long startTime = System.currentTimeMillis();
            int updateCount = 0;
            
            for (int i = 0; i < drivers.size(); i++) {
                Driver current = drivers.get(i);
                
                // Move driver (may change H3 cell)
                Driver updated = current.move(UPDATE_INTERVAL_SEC);
                drivers.set(i, updated);
                
                // Send to Kafka
                try {
                    String json = mapper.writeValueAsString(updated);
                    producer.send(new ProducerRecord<>(TOPIC, updated.id(), json));
                    updateCount++;
                } catch (Exception e) {
                    System.err.println("Failed to send update: " + e.getMessage());
                }
            }
            
            long elapsed = System.currentTimeMillis() - startTime;
            System.out.printf("[%s] Sent %d updates in %d ms (%.1f updates/sec)%n",
                Instant.now(), updateCount, elapsed, (updateCount * 1000.0 / elapsed));
                
        }, 0, UPDATE_INTERVAL_SEC, TimeUnit.SECONDS);

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            executor.shutdown();
            producer.close();
        }));

        System.out.println("Driver simulator running. Press Ctrl+C to stop.");
    }
}
