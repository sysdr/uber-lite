package com.uberlite.geohash.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uberlite.geohash.model.DriverLocation;
import com.uberlite.geohash.partitioner.GeohashPartitioner;
import com.uberlite.geohash.partitioner.H3Partitioner;
import com.uberlite.geohash.processor.PartitionSkewProcessor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GeohashLimitsDemo {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final ObjectMapper mapper = new ObjectMapper();
    
    // Global cities with varying latitudes
    private static final List<CityLocation> CITIES = List.of(
        new CityLocation("Singapore", 1.3, 103.8),
        new CityLocation("Mumbai", 19.0, 72.8),
        new CityLocation("New York", 40.7, -74.0),
        new CityLocation("London", 51.5, -0.1),
        new CityLocation("Oslo", 59.9, 10.7),
        new CityLocation("Helsinki", 60.2, 24.9),
        new CityLocation("Reykjavik", 64.1, -21.9),
        new CityLocation("Anchorage", 61.2, -149.9)
    );

    record CityLocation(String name, double lat, double lon) {}

    public static void main(String[] args) throws Exception {
        System.out.println("=== Lesson 5: Geohash Limits Demo ===\n");
        
        // Create topics
        createTopics();
        
        // Start Kafka Streams processors
        var streamsExecutor = Executors.newVirtualThreadPerTaskExecutor();
        streamsExecutor.submit(() -> startStreamsApp("geohash"));
        streamsExecutor.submit(() -> startStreamsApp("h3"));
        
        // Wait for streams to initialize
        Thread.sleep(5000);
        
        // Generate initial batch of driver locations
        generateDriverLocations();
        
        System.out.println("\n=== Initial Demo Complete ===");
        System.out.println("Streams apps are running. Producing continuous data...");
        System.out.println("Press Ctrl+C to stop");
        System.out.println("Dashboard: http://localhost:8080/dashboard\n");
        
        // Keep producing data continuously
        continuousDataProduction();
    }

    private static void createTopics() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        
        try (AdminClient admin = AdminClient.create(props)) {
            var topics = List.of(
                new NewTopic("driver-locations-geohash", 12, (short) 1),
                new NewTopic("driver-locations-h3", 12, (short) 1),
                new NewTopic("partition-metrics", 1, (short) 1)
            );
            
            admin.createTopics(topics);
            System.out.println("✓ Created topics");
        }
    }

    private static void startStreamsApp(String type) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "partition-skew-" + type);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        
        builder.stream("driver-locations-" + type, 
                      Consumed.with(Serdes.String(), Serdes.ByteArray()))
               .process(() -> new PartitionSkewProcessor(type))
               .to("partition-metrics");

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        
        streams.start();
        System.out.println("✓ Started Streams app: " + type);
        
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void continuousDataProduction() throws Exception {
        Random random = new Random();
        
        // Geohash producer
        KafkaProducer<String, byte[]> geohashProducer = createProducer(GeohashPartitioner.class.getName());
        
        // H3 producer
        KafkaProducer<String, byte[]> h3Producer = createProducer(H3Partitioner.class.getName());
        
        int counter = 5000; // Start from 5000 to continue from initial batch
        
        while (true) {
            // Randomly select a city
            var city = CITIES.get(random.nextInt(CITIES.size()));
            
            // Add small random offset (within ~5km)
            double lat = city.lat + (random.nextDouble() - 0.5) * 0.05;
            double lon = city.lon + (random.nextDouble() - 0.5) * 0.05;
            
            var location = new DriverLocation(
                "driver_" + counter,
                lat,
                lon,
                System.currentTimeMillis(),
                city.name
            );
            
            byte[] value = mapper.writeValueAsBytes(location);
            
            // Send to both topics
            geohashProducer.send(new ProducerRecord<>("driver-locations-geohash", location.driverId(), value));
            h3Producer.send(new ProducerRecord<>("driver-locations-h3", location.driverId(), value));
            
            counter++;
            
            if (counter % 10 == 0) {
                System.out.printf("Produced %d total locations...\n", counter);
            }
            
            // Wait 2 seconds between messages
            Thread.sleep(2000);
        }
    }

    private static void generateDriverLocations() throws Exception {
        Random random = new Random(42);
        
        // Geohash producer
        KafkaProducer<String, byte[]> geohashProducer = createProducer(GeohashPartitioner.class.getName());
        
        // H3 producer
        KafkaProducer<String, byte[]> h3Producer = createProducer(H3Partitioner.class.getName());
        
        System.out.println("\n=== Generating 5000 driver locations across latitudes ===\n");
        
        for (int i = 0; i < 5000; i++) {
            // Randomly select a city
            var city = CITIES.get(random.nextInt(CITIES.size()));
            
            // Add small random offset (within ~5km)
            double lat = city.lat + (random.nextDouble() - 0.5) * 0.05;
            double lon = city.lon + (random.nextDouble() - 0.5) * 0.05;
            
            var location = new DriverLocation(
                "driver_" + i,
                lat,
                lon,
                System.currentTimeMillis(),
                city.name
            );
            
            byte[] value = mapper.writeValueAsBytes(location);
            
            // Send to both topics
            geohashProducer.send(new ProducerRecord<>("driver-locations-geohash", location.driverId(), value));
            h3Producer.send(new ProducerRecord<>("driver-locations-h3", location.driverId(), value));
            
            if ((i + 1) % 1000 == 0) {
                System.out.printf("Sent %d locations...\n", i + 1);
            }
        }
        
        geohashProducer.flush();
        h3Producer.flush();
        geohashProducer.close();
        h3Producer.close();
        
        System.out.println("\n✓ Generated 5000 locations");
        
        // Wait for processing
        Thread.sleep(10000);
    }

    private static KafkaProducer<String, byte[]> createProducer(String partitionerClass) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionerClass);
        
        return new KafkaProducer<>(props);
    }
}
