package com.uberlite.firehose.topology;

import com.uberlite.firehose.config.FirehoseRocksDBConfig;
import com.uberlite.firehose.config.KafkaTopicAdmin;
import com.uberlite.firehose.metrics.MetricsRegistry;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

/**
 * Raw Topology (Processor API) — no DSL, no StreamsBuilder.
 *
 * Topology graph:
 *   SOURCE: driver-locations (12 partitions)
 *     └─> PROCESSOR: location-processor
 *           └─> STATE STORE: driver-cell-store (RocksDB, persistent)
 *
 * 4 StreamThreads: each thread owns 3 partitions and 3 RocksDB instances.
 * All writes within a partition are serialized — no cross-thread state contention.
 */
public class FirehoseTopology {

    private final MetricsRegistry metrics;

    public FirehoseTopology(MetricsRegistry metrics) {
        this.metrics = metrics;
    }

    public Topology build() {
        var topology = new Topology();

        // State store: persistent RocksDB with custom tuning
        var storeSupplier = Stores.persistentKeyValueStore(DriverLocationProcessor.STORE_NAME);
        var storeBuilder  = Stores.keyValueStoreBuilder(
                storeSupplier,
                Serdes.String(),  // key: driverId
                Serdes.Long()     // value: H3 cell index
        ).withCachingEnabled();   // Kafka Streams row cache (before RocksDB writes)

        topology
            .addSource(
                "driver-source",
                new StringDeserializer(),
                new ByteArrayDeserializer(),
                KafkaTopicAdmin.DRIVER_LOCATIONS_TOPIC
            )
            .addProcessor(
                "location-processor",
                () -> new DriverLocationProcessor(metrics),
                "driver-source"
            )
            .addStateStore(storeBuilder, "location-processor");

        return topology;
    }

    public Properties streamsConfig() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,             "firehose-capstone-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,          "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,         4);
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, FirehoseRocksDBConfig.class.getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,         100);  // Flush offsets every 100ms
        props.put(StreamsConfig.POLL_MS_CONFIG,                    50);   // Consumer poll interval
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,  10 * 1024 * 1024L); // 10MB row cache
        // Consumer fetch tuning for high-throughput topic
        props.put("fetch.min.bytes",          "65536");   // Wait for 64KB before returning
        props.put("fetch.max.wait.ms",        "500");
        props.put("max.poll.records",         "2000");    // Pull 2k records per poll
        props.put("max.partition.fetch.bytes","2097152"); // 2MB per partition per fetch
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        return props;
    }
}
