package com.uberlite.streams;

import com.uberlite.common.DriverLocation;
import com.uberlite.common.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Baseline Kafka Streams application.
 * Future lessons will add RocksDB state stores and H3-based partitioning.
 */
public class GeoIndexApp {
    private static final Logger log = LoggerFactory.getLogger(GeoIndexApp.class);

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geo-index-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        
        var builder = new StreamsBuilder();
        
        builder.stream("driver-locations", 
            Consumed.with(Serdes.String(), JsonSerde.serde(DriverLocation.class)))
            .peek((key, value) -> log.info("Received: driver={}, h3={}", 
                value.driverId(), Long.toHexString(value.h3Index())));
        
        var streams = new KafkaStreams(builder.build(), props);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down gracefully...");
            streams.close();
        }));
        
        streams.start();
        log.info("Kafka Streams application started");
    }
}
