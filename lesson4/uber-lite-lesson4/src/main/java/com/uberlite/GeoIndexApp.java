package com.uberlite;

import com.uber.h3core.H3Core;
import com.uberlite.model.DriverLocation;
import com.uberlite.partitioner.H3Partitioner;
import com.uberlite.processor.SpatialIndexProcessor;
import com.uberlite.serdes.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

import java.io.IOException;
import java.util.Properties;

public class GeoIndexApp {
    
    public static void main(String[] args) throws IOException {
        var h3 = H3Core.newInstance();
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "geo-indexing-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        
        // RocksDB tuning for range queries
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, RocksDBConfigSetter.class);
        
        var builder = new StreamsBuilder();
        
        // Input: driver locations partitioned by driver_id
        KStream<String, DriverLocation> driverStream = builder.stream(
            "driver-locations",
            Consumed.with(Serdes.String(), JsonSerdes.driverLocation())
        );
        
        // Define state store FIRST (before processor uses it)
        StoreBuilder<KeyValueStore<Bytes, byte[]>> storeBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("spatial-index"),
            Serdes.Bytes(),
            Serdes.ByteArray()
        ).withCachingEnabled();
        
        builder.addStateStore(storeBuilder);
        
        // Transform: add H3 index as key
        KStream<Long, DriverLocation> h3Stream = driverStream.map((driverId, location) -> {
            long h3Index = h3.latLngToCell(location.lat(), location.lon(), 9);
            return KeyValue.pair(h3Index, location);
        });
        
        // Repartition by H3 index using custom partitioner
        h3Stream.through(
            "driver-locations-by-h3",
            Produced.with(Serdes.Long(), JsonSerdes.driverLocation(), new H3Partitioner())
        ).process(
            SpatialIndexProcessor::new,
            Named.as("spatial-indexer"),
            "spatial-index"
        );
        
        var topology = builder.build();
        System.out.println(topology.describe());
        
        var streams = new KafkaStreams(topology, props);
        
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
        
        System.out.println("✅ Geospatial indexing stream running...");
    }
}
