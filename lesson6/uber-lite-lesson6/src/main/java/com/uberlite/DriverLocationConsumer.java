package com.uberlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static com.uberlite.Models.*;

public class DriverLocationConsumer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DriverLocationConsumer.class);
    private final KafkaConsumer<String, String> consumer;
    private final H3SpatialIndex spatialIndex;
    private final ObjectMapper mapper = new ObjectMapper();
    private volatile boolean running = true;
    
    public DriverLocationConsumer(String bootstrapServers, H3SpatialIndex spatialIndex) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "h3-indexer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        
        this.consumer = new KafkaConsumer<>(props);
        this.spatialIndex = spatialIndex;
    }
    
    public void start() {
        consumer.subscribe(List.of("driver-locations"));
        
        Thread.ofVirtual().start(() -> {
            log.info("Driver location consumer started");
            
            while (running) {
                try {
                    var records = consumer.poll(Duration.ofMillis(100));
                    
                    for (var record : records) {
                        var update = mapper.readValue(record.value(), DriverLocationUpdate.class);
                        spatialIndex.indexDriverLocation(update);
                    }
                    
                    if (!records.isEmpty()) {
                        log.debug("Indexed {} driver location updates", records.count());
                    }
                    
                } catch (Exception e) {
                    log.error("Error consuming driver locations", e);
                }
            }
        });
    }
    
    @Override
    public void close() {
        running = false;
        consumer.close();
    }
}
