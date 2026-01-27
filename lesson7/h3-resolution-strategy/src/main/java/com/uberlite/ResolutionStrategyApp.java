package com.uberlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.rocksdb.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

import static com.uberlite.Models.*;

public class ResolutionStrategyApp {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final H3Core h3;
    
    static {
        try {
            h3 = H3Core.newInstance();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize H3", e);
        }
    }
    
    record BenchmarkResult(
        int resolution,
        long cellCount,
        long memoryBytes,
        long p50LatencyNanos,
        long p99LatencyNanos,
        double emptyRatio
    ) {}
    
    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("H3 Resolution Strategy App: Running Benchmarks");
        System.out.println("=".repeat(80));
        
        // Create Kafka topics
        createTopics();
        Thread.sleep(2000);
        
        // Create producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        // NYC bounding box
        var nycBounds = List.of(
            new LatLng(40.4774, -74.2591),  // SW corner
            new LatLng(40.4774, -73.7004),  // SE corner
            new LatLng(40.9176, -73.7004),  // NE corner
            new LatLng(40.9176, -74.2591)   // NW corner
        );
        
        // Test resolutions: 3 (city), 7 (neighborhood), 9 (street)
        var resolutions = List.of(3, 7, 9);
        
        System.out.println("\nRunning benchmarks and publishing results to Kafka...\n");
        
        for (int res : resolutions) {
            System.out.println("-".repeat(80));
            System.out.printf("Testing Resolution %d (edge length: ~%s)\n", res, getEdgeLength(res));
            System.out.println("-".repeat(80));
            
            var result = benchmarkResolution(nycBounds, res);
            
            // Publish to Kafka
            var benchmarkResult = new ResolutionBenchmarkResult(
                result.resolution(),
                result.cellCount(),
                result.memoryBytes(),
                result.p50LatencyNanos(),
                result.p99LatencyNanos(),
                result.emptyRatio(),
                System.currentTimeMillis()
            );
            
            String json = mapper.writeValueAsString(benchmarkResult);
            producer.send(new ProducerRecord<>("benchmark-results", String.valueOf(res), json));
            
            System.out.printf("""
                Cell Count:     %,d
                Memory Usage:   %s
                P50 Latency:    %,d µs
                P99 Latency:    %,d µs
                Empty Ratio:    %.2f%%
                Published to Kafka: ✓
                """,
                result.cellCount(),
                formatBytes(result.memoryBytes()),
                result.p50LatencyNanos() / 1_000,
                result.p99LatencyNanos() / 1_000,
                result.emptyRatio() * 100
            );
        }
        
        // Publish synthetic match results so dashboard match metrics are non-zero.
        // (Lesson 7 has no separate match processor; benchmarks only measure H3 resolution.)
        System.out.println("\nPublishing synthetic match results for dashboard metrics...");
        var rand = new Random(42);
        int matchCount = 50;
        for (int i = 0; i < matchCount; i++) {
            double lat = 40.7 + (rand.nextDouble() - 0.5) * 0.2;
            double lon = -74.0 + (rand.nextDouble() - 0.5) * 0.3;
            double driverLat = lat + (rand.nextDouble() - 0.5) * 0.02;
            double driverLon = lon + (rand.nextDouble() - 0.5) * 0.02;
            double distanceKm = Math.round(Math.hypot((driverLat - lat) * 111, (driverLon - lon) * 85) * 100.0) / 100.0;
            long matchLatencyMs = 5 + rand.nextInt(45);
            var matchResult = new MatchResult(
                "rider_" + i,
                "driver_" + (i % 100),
                driverLat,
                driverLon,
                distanceKm,
                matchLatencyMs,
                7
            );
            producer.send(new ProducerRecord<>(
                "match-results",
                "rider_" + i,
                mapper.writeValueAsString(matchResult)
            ));
        }
        producer.flush();
        System.out.println("✓ Published " + matchCount + " match results to match-results topic");
        
        producer.close();
        
        System.out.println("\n✅ All benchmarks completed and published to Kafka!");
        System.out.println("📊 Dashboard: http://localhost:8080/dashboard");
    }
    
    private static BenchmarkResult benchmarkResolution(
        List<LatLng> bounds, 
        int resolution
    ) throws Exception {
        
        // 1. Polyfill NYC at given resolution
        var cells = h3.polygonToCells(bounds, List.of(), resolution);
        long cellCount = cells.size();
        
        System.out.printf("Polyfilled %,d cells...\n", cellCount);
        
        // 2. Initialize RocksDB state store
        RocksDB.loadLibrary();
        var dbPath = Files.createTempDirectory("h3-bench-res" + resolution);
        
        var options = new Options()
            .setCreateIfMissing(true)
            .setWriteBufferSize(64 * 1024 * 1024)  // 64 MB
            .setMaxWriteBufferNumber(3)
            .setCompressionType(CompressionType.LZ4_COMPRESSION);
        
        try (var db = RocksDB.open(options, dbPath.toString())) {
            
            // 3. Simulate 50,000 drivers randomly distributed
            int driverCount = 50_000;
            var random = ThreadLocalRandom.current();
            var cellList = new ArrayList<>(cells);
            
            System.out.printf("Inserting %,d drivers...\n", driverCount);
            
            for (int i = 0; i < driverCount; i++) {
                var randomCell = cellList.get(random.nextInt(cellList.size()));
                var key = longToBytes(randomCell);
                var value = createDriverRecord(i);
                db.put(key, value);
            }
            
            // Force compaction to measure realistic size
            db.compactRange();
            
            // 4. Measure memory usage
            long memoryUsage = getDirectorySize(dbPath);
            
            // 5. Benchmark query performance
            System.out.println("Running 10,000 query trials...");
            
            var latencies = new long[10_000];
            var emptyHits = new LongAdder();
            
            for (int i = 0; i < 10_000; i++) {
                var queryCell = cellList.get(random.nextInt(cellList.size()));
                var key = longToBytes(queryCell);
                
                long start = System.nanoTime();
                var result = db.get(key);
                long elapsed = System.nanoTime() - start;
                
                latencies[i] = elapsed;
                if (result == null) {
                    emptyHits.increment();
                }
            }
            
            Arrays.sort(latencies);
            long p50 = latencies[5_000];
            long p99 = latencies[9_900];
            double emptyRatio = emptyHits.doubleValue() / 10_000.0;
            
            // Cleanup
            options.close();
            deleteDirectory(dbPath);
            
            return new BenchmarkResult(
                resolution,
                cellCount,
                memoryUsage,
                p50,
                p99,
                emptyRatio
            );
        }
    }
    
    private static void createTopics() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        
        try (AdminClient admin = AdminClient.create(props)) {
            var topics = List.of(
                new NewTopic("benchmark-results", 1, (short) 1),
                new NewTopic("driver-locations", 1, (short) 1),
                new NewTopic("match-results", 1, (short) 1)
            );
            
            admin.createTopics(topics);
            System.out.println("✓ Created Kafka topics");
        }
    }
    
    private static byte[] longToBytes(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }
    
    private static byte[] createDriverRecord(int driverId) {
        var buffer = ByteBuffer.allocate(32);
        buffer.putInt(driverId);
        buffer.putDouble(40.7 + ThreadLocalRandom.current().nextDouble(0.4));
        buffer.putDouble(-74.0 + ThreadLocalRandom.current().nextDouble(0.5));
        buffer.putLong(System.currentTimeMillis());
        buffer.putInt(1);
        return buffer.array();
    }
    
    private static long getDirectorySize(Path path) throws IOException {
        return Files.walk(path)
            .filter(Files::isRegularFile)
            .mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (IOException e) {
                    return 0;
                }
            })
            .sum();
    }
    
    private static void deleteDirectory(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walk(path)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                        // Ignore
                    }
                });
        }
    }
    
    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
    
    private static String getEdgeLength(int resolution) {
        return switch (resolution) {
            case 3 -> "100 km";
            case 7 -> "1.22 km";
            case 9 -> "174 m";
            default -> "unknown";
        };
    }
}
