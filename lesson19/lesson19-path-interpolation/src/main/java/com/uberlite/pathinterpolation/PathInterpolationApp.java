package com.uberlite.pathinterpolation;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class PathInterpolationApp {
    private static final Logger log = LoggerFactory.getLogger(PathInterpolationApp.class);
    
    private static final String INPUT_TOPIC = "driver-locations";
    private static final String STATE_STORE_NAME = "cell-driver-index";
    
    public static void main(String[] args) {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "path-interpolation-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        // RocksDB optimization
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, 
                  RocksDBConfig.class.getName());
        
        // Enable exactly-once semantics
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, 
                  StreamsConfig.EXACTLY_ONCE_V2);
        
        var topology = buildTopology();
        var streams = new KafkaStreams(topology, props);
        
        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            streams.close();
        }));
        
        log.info("Starting PathInterpolationApp...");
        streams.start();
    }
    
    private static Topology buildTopology() {
        var builder = new StreamsBuilder();
        
        // Define state store
        StoreBuilder<KeyValueStore<String, CellEntry>> storeBuilder = 
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATE_STORE_NAME),
                Serdes.String(),
                new JsonSerde<>(CellEntry.class)
            ).withLoggingEnabled(null); // Enable changelog
        
        builder.addStateStore(storeBuilder);
        
        // Build topology
        builder.<String, DriverLocationUpdate>stream(INPUT_TOPIC, 
                org.apache.kafka.streams.kstream.Consumed.with(
                    Serdes.String(),
                    new JsonSerde<>(DriverLocationUpdate.class)
                ))
            .process(
                PathInterpolationProcessor::new,
                STATE_STORE_NAME
            );
        
        return builder.build();
    }
}
