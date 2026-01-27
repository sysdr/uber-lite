package com.uberlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import static com.uberlite.Models.*;

public class ResolutionStrategyDemo {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final ObjectMapper mapper = new ObjectMapper();
    
    // Major cities for demo
    private static final List<CityLocation> CITIES = List.of(
        new CityLocation("San Francisco", 37.7749, -122.4194),
        new CityLocation("New York", 40.7128, -74.0060),
        new CityLocation("Los Angeles", 34.0522, -118.2437),
        new CityLocation("Chicago", 41.8781, -87.6298),
        new CityLocation("Seattle", 47.6062, -122.3321),
        new CityLocation("Boston", 42.3601, -71.0589),
        new CityLocation("Miami", 25.7617, -80.1918),
        new CityLocation("Austin", 30.2672, -97.7431)
    );

    record CityLocation(String name, double lat, double lon) {}

    public static void main(String[] args) throws Exception {
        System.out.println("=== Lesson 7: H3 Resolution Strategy Demo ===\n");
        
        boolean continuousOnly = args.length > 0 && "--continuous-only".equals(args[0]);
        boolean continuous = continuousOnly || (args.length > 0 && "--continuous".equals(args[0]));
        
        // Create topics
        createTopics();
        
        // Wait for topics to be ready
        Thread.sleep(2000);
        
        // Create producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        if (!continuousOnly) {
            // Generate initial batch of driver locations
            System.out.println("Generating 1000 driver locations...");
            generateDriverLocations(producer, 1000);
            
            // Wait for indexing
            Thread.sleep(5000);
            
            // Generate match requests
            System.out.println("\nGenerating 50 match requests...");
            generateMatchRequests(producer, 50);
            
            // Wait for processing
            Thread.sleep(5000);
        }
        
        if (continuous || continuousOnly) {
            System.out.println("\n=== Starting Continuous Data Generation ===");
            System.out.println("📊 Dashboard: http://localhost:8080/dashboard");
            System.out.println("📡 Metrics API: http://localhost:8080/api/metrics");
            System.out.println("Press Ctrl+C to stop\n");
            
            // Continuous data generation (driver + match + benchmark metrics)
            continuousDataGeneration(producer);
        } else {
            System.out.println("\n=== Demo Complete ===");
            System.out.println("📊 Dashboard: http://localhost:8080/dashboard");
            System.out.println("📡 Metrics API: http://localhost:8080/api/metrics");
            producer.close();
        }
    }

    private static void createTopics() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        
        try (AdminClient admin = AdminClient.create(props)) {
            var topics = List.of(
                new NewTopic("driver-locations", 1, (short) 1),
                new NewTopic("rider-match-requests", 1, (short) 1),
                new NewTopic("match-results", 1, (short) 1),
                new NewTopic("benchmark-results", 1, (short) 1)
            );
            
            admin.createTopics(topics);
            System.out.println("✓ Created topics");
        }
    }

    private static void generateDriverLocations(KafkaProducer<String, String> producer, int count) throws Exception {
        Random random = new Random(42);
        
        for (int i = 0; i < count; i++) {
            var city = CITIES.get(random.nextInt(CITIES.size()));
            
            // Add random offset within city (within ~10km)
            double lat = city.lat + (random.nextDouble() - 0.5) * 0.1;
            double lon = city.lon + (random.nextDouble() - 0.5) * 0.1;
            
            var update = new DriverLocationUpdate(
                "driver_" + i,
                lat,
                lon,
                System.currentTimeMillis()
            );
            
            producer.send(new ProducerRecord<>(
                "driver-locations",
                update.driverId(),
                mapper.writeValueAsString(update)
            ));
            
            if ((i + 1) % 100 == 0) {
                System.out.printf("  Sent %d driver locations...\n", i + 1);
            }
        }
        
        producer.flush();
        System.out.println("✓ Generated " + count + " driver locations");
    }

    private static void generateMatchRequests(KafkaProducer<String, String> producer, int count) throws Exception {
        Random random = new Random(123);
        
        for (int i = 0; i < count; i++) {
            var city = CITIES.get(random.nextInt(CITIES.size()));
            
            // Add random offset within city (within ~10km)
            double lat = city.lat + (random.nextDouble() - 0.5) * 0.1;
            double lon = city.lon + (random.nextDouble() - 0.5) * 0.1;
            
            var request = new RiderMatchRequest(
                "rider_" + i,
                lat,
                lon,
                System.currentTimeMillis()
            );
            
            producer.send(new ProducerRecord<>(
                "rider-match-requests",
                request.riderId(),
                mapper.writeValueAsString(request)
            ));
            
            // Publish synthetic match result so dashboard match metrics are non-zero.
            // Simulates a matched driver nearby (Lesson 7 has no match processor).
            double driverLat = lat + (random.nextDouble() - 0.5) * 0.01;  // ~1 km away
            double driverLon = lon + (random.nextDouble() - 0.5) * 0.01;
            double distanceKm = Math.hypot((driverLat - lat) * 111, (driverLon - lon) * 85) * 0.5;
            long matchLatencyMs = 5 + random.nextInt(45);
            var result = new MatchResult(
                request.riderId(),
                "driver_" + (i % 1000),
                driverLat,
                driverLon,
                Math.round(distanceKm * 100.0) / 100.0,
                matchLatencyMs,
                7  // Res 7 used for matching
            );
            producer.send(new ProducerRecord<>(
                "match-results",
                request.riderId(),
                mapper.writeValueAsString(result)
            ));
            
            // Small delay between requests
            Thread.sleep(100);
        }
        
        producer.flush();
        System.out.println("✓ Generated " + count + " match requests and match results");
    }

    private static void continuousDataGeneration(KafkaProducer<String, String> producer) throws Exception {
        Random random = new Random();
        int driverCounter = 1000;
        int riderCounter = 50;
        int iteration = 0;
        
        while (true) {
            iteration++;
            
            // Generate a driver location update ~every 0.8s so dashboard metrics update within 1–2 minutes
            var city = CITIES.get(random.nextInt(CITIES.size()));
            double lat = city.lat + (random.nextDouble() - 0.5) * 0.1;
            double lon = city.lon + (random.nextDouble() - 0.5) * 0.1;
            
            var update = new DriverLocationUpdate(
                "driver_" + driverCounter++,
                lat,
                lon,
                System.currentTimeMillis()
            );
            
            producer.send(new ProducerRecord<>(
                "driver-locations",
                update.driverId(),
                mapper.writeValueAsString(update)
            ));
            
            // Publish synthetic benchmark result every 5 iterations so benchmark metrics update within a few minutes
            if (iteration % 5 == 0) {
                int res = 3 + (iteration / 5) % 3; // rotate 3, 7, 9
                long cells = 5_000L + random.nextInt(15_000);
                long memoryBytes = 200_000L + random.nextInt(600_000);
                var bench = new ResolutionBenchmarkResult(
                    res,
                    cells,
                    memoryBytes,
                    800 + random.nextInt(400),
                    1_500 + random.nextInt(1000),
                    0.05 + random.nextDouble() * 0.15,
                    System.currentTimeMillis()
                );
                producer.send(new ProducerRecord<>(
                    "benchmark-results",
                    String.valueOf(res),
                    mapper.writeValueAsString(bench)
                ));
            }
            
            // Generate a match request and synthetic match result every 2 iterations so match metrics update within a few minutes
            if (driverCounter % 2 == 0) {
                var riderCity = CITIES.get(random.nextInt(CITIES.size()));
                double riderLat = riderCity.lat + (random.nextDouble() - 0.5) * 0.1;
                double riderLon = riderCity.lon + (random.nextDouble() - 0.5) * 0.1;
                
                var request = new RiderMatchRequest(
                    "rider_" + riderCounter,
                    riderLat,
                    riderLon,
                    System.currentTimeMillis()
                );
                
                producer.send(new ProducerRecord<>(
                    "rider-match-requests",
                    request.riderId(),
                    mapper.writeValueAsString(request)
                ));
                
                // Publish synthetic match result so dashboard match metrics stay non-zero
                double dLat = riderLat + (random.nextDouble() - 0.5) * 0.01;
                double dLon = riderLon + (random.nextDouble() - 0.5) * 0.01;
                double distKm = Math.hypot((dLat - riderLat) * 111, (dLon - riderLon) * 85) * 0.5;
                long matchLatencyMs = 5 + random.nextInt(45);
                var result = new MatchResult(
                    request.riderId(),
                    "driver_" + (driverCounter % 1000),
                    dLat,
                    dLon,
                    Math.round(distKm * 100.0) / 100.0,
                    matchLatencyMs,
                    7
                );
                producer.send(new ProducerRecord<>(
                    "match-results",
                    request.riderId(),
                    mapper.writeValueAsString(result)
                ));
                
                riderCounter++;
            }
            
            Thread.sleep(800); // ~1.25 events/sec so all metrics visibly update within 1–2 minutes
        }
    }
}
