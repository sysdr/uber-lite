package com.uberlite.concurrency;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.h3core.H3Core;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class VirtualThreadSimulator {
    private static final int TOTAL_DRIVERS = 10_000;
    private static final int VIRTUAL_THREADS = 100;
    private static final int DRIVERS_PER_THREAD = TOTAL_DRIVERS / VIRTUAL_THREADS;
    
    // NYC bounding box
    private static final double NYC_LAT_MIN = 40.6;
    private static final double NYC_LAT_MAX = 40.9;
    private static final double NYC_LON_MIN = -74.1;
    private static final double NYC_LON_MAX = -73.8;
    
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong TOTAL_SENT = new AtomicLong(0);
    private static final Map<String, Long> THREAD_METRICS = new ConcurrentHashMap<>();
    
    public static void main(String[] args) throws Exception {
        var h3 = H3Core.newInstance();
        var producer = createProducer();
        
        // Create topic if not exists
        System.out.println("🔧 Initializing...");
        Thread.sleep(2000); // Wait for Kafka
        
        // Initialize drivers
        var allDrivers = initializeDrivers(h3);
        
        // Partition drivers across virtual threads
        var driverBatches = partitionDrivers(allDrivers);
        
        // Start metrics reporter
        startMetricsReporter();
        
        // Launch virtual threads
        System.out.println("🚀 Launching " + VIRTUAL_THREADS + " virtual threads...");
        var threads = new ArrayList<Thread>();
        
        for (int threadId = 0; threadId < VIRTUAL_THREADS; threadId++) {
            var drivers = driverBatches.get(threadId);
            var threadName = "VT-" + threadId;
            
            Thread vt = Thread.ofVirtual().name(threadName).start(() -> {
                runSimulationLoop(threadName, drivers, producer);
            });
            threads.add(vt);
        }
        
        // Keep main thread alive
        for (var thread : threads) {
            thread.join();
        }
    }
    
    private static KafkaProducer<String, String> createProducer() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Batching configuration
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); // 64KB
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 134217728); // 128MB
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        
        return new KafkaProducer<>(props);
    }
    
    private static List<DriverState> initializeDrivers(H3Core h3) throws IOException {
        var drivers = new ArrayList<DriverState>();
        for (int i = 0; i < TOTAL_DRIVERS; i++) {
            double lat = NYC_LAT_MIN + Math.random() * (NYC_LAT_MAX - NYC_LAT_MIN);
            double lon = NYC_LON_MIN + Math.random() * (NYC_LON_MAX - NYC_LON_MIN);
            long h3Index = h3.latLngToCell(lat, lon, 9);
            
            drivers.add(new DriverState(
                "driver-" + String.format("%05d", i),
                lat,
                lon,
                h3Index,
                System.currentTimeMillis()
            ));
        }
        return drivers;
    }
    
    private static List<List<DriverState>> partitionDrivers(List<DriverState> drivers) {
        var batches = new ArrayList<List<DriverState>>();
        for (int i = 0; i < VIRTUAL_THREADS; i++) {
            int start = i * DRIVERS_PER_THREAD;
            int end = start + DRIVERS_PER_THREAD;
            batches.add(new ArrayList<>(drivers.subList(start, end)));
        }
        return batches;
    }
    
    private static void runSimulationLoop(
        String threadName,
        List<DriverState> drivers,
        KafkaProducer<String, String> producer
    ) {
        long iteration = 0;
        while (true) {
            try {
                long cycleStart = System.currentTimeMillis();
                
                // Update all drivers in this thread's batch
                for (int i = 0; i < drivers.size(); i++) {
                    var driver = drivers.get(i).moveRandomly();
                    drivers.set(i, driver);
                    
                    var update = new DriverLocationUpdate(
                        driver.driverId(),
                        driver.lat(),
                        driver.lon(),
                        driver.h3Index(),
                        driver.lastUpdateMs()
                    );
                    
                    var json = MAPPER.writeValueAsString(update);
                    producer.send(
                        new ProducerRecord<>("driver-locations", driver.driverId(), json),
                        (metadata, exception) -> {
                            if (exception == null) {
                                TOTAL_SENT.incrementAndGet();
                            }
                        }
                    );
                }
                
                iteration++;
                THREAD_METRICS.put(threadName, iteration);
                
                // Sleep for remainder of 1-second cycle
                long elapsed = System.currentTimeMillis() - cycleStart;
                long sleepTime = Math.max(0, 1000 - elapsed);
                Thread.sleep(Duration.ofMillis(sleepTime));
                
            } catch (Exception e) {
                System.err.println("[" + threadName + "] Error: " + e.getMessage());
            }
        }
    }
    
    private static void startMetricsReporter() {
        Thread.ofVirtual().start(() -> {
            long lastTotal = 0;
            while (true) {
                try {
                    Thread.sleep(Duration.ofSeconds(5));
                    
                    long currentTotal = TOTAL_SENT.get();
                    long rate = (currentTotal - lastTotal) / 5;
                    lastTotal = currentTotal;
                    
                    System.out.printf("📊 Rate: %,d records/sec | Total: %,d | Active VTs: %d%n",
                        rate, currentTotal, THREAD_METRICS.size());
                    
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
    }
}
