package com.uberlite.pathinterpolation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.h3core.H3Core;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class DataGenerator {
    
    public static void main(String[] args) throws Exception {
        var h3 = H3Core.newInstance();
        var mapper = new ObjectMapper();
        
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        
        var producer = new KafkaProducer<String, String>(props);
        
        // Simulate driver moving in Manhattan
        var startLat = 40.7580;
        var startLon = -73.9855; // Times Square
        var driverId = "driver-001";
        
        System.out.println("Generating 100 location updates...");
        
        for (int i = 0; i < 100; i++) {
            // Move north (0.0001 degrees ≈ 11 meters)
            var lat = startLat + (i * 0.0001);
            var lon = startLon;
            
            var h3Cell = h3.latLngToCell(lat, lon, 9);
            var timestamp = System.currentTimeMillis();
            
            var update = new DriverLocationUpdate(driverId, h3Cell, timestamp, lat, lon);
            var json = mapper.writeValueAsString(update);
            
            var record = new ProducerRecord<String, String>(
                "driver-locations",
                driverId,
                json
            );
            
            producer.send(record);
            
            if (i % 10 == 0) {
                System.out.printf("Sent update %d: cell=%s, lat=%.6f%n", i, 
                                  Long.toHexString(h3Cell), lat);
            }
            
            Thread.sleep(100); // 10 updates/sec
        }
        
        producer.flush();
        producer.close();
        System.out.println("Done!");
    }
}
