package com.uberlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static com.uberlite.Models.*;

public class MatchRequestHandler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MatchRequestHandler.class);
    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final H3SpatialIndex spatialIndex;
    private final ObjectMapper mapper = new ObjectMapper();
    private volatile boolean running = true;
    
    public MatchRequestHandler(String bootstrapServers, H3SpatialIndex spatialIndex) {
        var consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "match-handler");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        var producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.producer = new KafkaProducer<>(producerProps);
        this.spatialIndex = spatialIndex;
    }
    
    public void start() {
        consumer.subscribe(List.of("rider-match-requests"));
        
        Thread.ofVirtual().start(() -> {
            log.info("Match request handler started");
            
            while (running) {
                try {
                    var records = consumer.poll(Duration.ofMillis(100));
                    
                    for (var record : records) {
                        long startTime = System.currentTimeMillis();
                        
                        var request = mapper.readValue(record.value(), RiderMatchRequest.class);
                        var nearbyDrivers = spatialIndex.findNearbyDrivers(request.lat(), request.lon());
                        
                        if (!nearbyDrivers.isEmpty()) {
                            // Simple: pick closest driver by actual distance
                            var closestDriver = nearbyDrivers.stream()
                                .min((d1, d2) -> Double.compare(
                                    haversineDistance(request.lat(), request.lon(), d1.lat(), d1.lon()),
                                    haversineDistance(request.lat(), request.lon(), d2.lat(), d2.lon())
                                ))
                                .orElseThrow();
                            
                            double distance = haversineDistance(
                                request.lat(), request.lon(),
                                closestDriver.lat(), closestDriver.lon()
                            );
                            
                            long latency = System.currentTimeMillis() - startTime;
                            
                            var result = new MatchResult(
                                request.riderId(),
                                closestDriver.driverId(),
                                closestDriver.lat(),
                                closestDriver.lon(),
                                distance,
                                latency
                            );
                            
                            producer.send(new ProducerRecord<>(
                                "match-results",
                                request.riderId(),
                                mapper.writeValueAsString(result)
                            ));
                            
                            log.info("Matched {} to {} (distance: {:.2f}km, latency: {}ms)",
                                request.riderId(), closestDriver.driverId(), String.format("%.2f", distance), latency);
                        }
                    }
                    
                } catch (Exception e) {
                    log.error("Error handling match request", e);
                }
            }
        });
    }
    
    private double haversineDistance(double lat1, double lon1, double lat2, double lon2) {
        final double R = 6371; // Earth radius in km
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                   Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                   Math.sin(dLon / 2) * Math.sin(dLon / 2);
        
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }
    
    @Override
    public void close() {
        running = false;
        consumer.close();
        producer.close();
    }
}
