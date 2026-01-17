package com.uberlite.lesson01.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uberlite.lesson01.model.DriverLocation;
import com.uberlite.lesson01.model.LocationEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Properties;

public class LocationStateProcessor {
    private final KafkaStreams streams;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final String STORE_NAME = "driver-locations-store";

    public LocationStateProcessor(String bootstrapServers) {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "location-state-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "4");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
        
        // RocksDB tuning
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, 
                  "com.uberlite.lesson01.kafka.CustomRocksDbConfig");
        
        var builder = new StreamsBuilder();
        
        var locations = builder.stream("location-updates", 
            Consumed.with(Serdes.String(), Serdes.String()));
        
        locations
            .groupByKey()
            .aggregate(
                () -> null,
                (driverId, eventJson, current) -> {
                    try {
                        var event = objectMapper.readValue(eventJson, LocationEvent.class);
                        return objectMapper.writeValueAsString(
                            new DriverLocation(
                                event.driverId(),
                                event.latitude(),
                                event.longitude(),
                                event.timestamp()
                            )
                        );
                    } catch (Exception e) {
                        return current;
                    }
                },
                Materialized.as(STORE_NAME)
            );
        
        this.streams = new KafkaStreams(builder.build(), props);
    }

    public void start() {
        streams.start();
    }

    public DriverLocation getDriverLocation(String driverId) {
        try {
            var store = streams.store(
                org.apache.kafka.streams.StoreQueryParameters.fromNameAndType(
                    STORE_NAME,
                    QueryableStoreTypes.<String, String>keyValueStore()
                )
            );
            
            var json = store.get(driverId);
            if (json != null) {
                return objectMapper.readValue(json, DriverLocation.class);
            }
        } catch (Exception e) {
            // Store not ready or key not found
        }
        return null;
    }

    public void close() {
        streams.close();
    }
}
