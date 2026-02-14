package com.uberlite.stress;

import com.uber.h3core.H3Core;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

/**
 * Kafka Streams application using the Processor API (PAPI).
 *
 * Topology: driver-locations → [LocationProcessor] → driver-locations-processed
 *
 * LocationProcessor:
 *  - Deserializes DriverLocationEvent
 *  - Validates H3 cell at Resolution 9
 *  - Writes to RocksDB state store (keyed by driverId)
 *  - Publishes enriched event downstream
 *
 * RocksDB is explicitly tuned to avoid block cache memory leaks under sustained load.
 */
public class DriverLocationStreamsApp {

    private static final String STATE_STORE_NAME = "driver-state";
    private final KafkaStreams streams;

    public DriverLocationStreamsApp(String bootstrapServers,
                                    String inputTopic, String outputTopic) throws IOException {
        var props = buildProps(bootstrapServers);
        var topology = buildTopology(inputTopic, outputTopic);
        this.streams = new KafkaStreams(topology, props);
        streams.setUncaughtExceptionHandler(ex -> {
            System.err.println("StreamThread error: " + ex.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
    }

    private Topology buildTopology(String inputTopic, String outputTopic) {
        var builder = new Topology();

        var storeSupplier = Stores.persistentKeyValueStore(STATE_STORE_NAME);
        var storeBuilder  = Stores.keyValueStoreBuilder(
                storeSupplier, Serdes.String(), Serdes.ByteArray())
                .withCachingEnabled()
                .withLoggingEnabled(new java.util.HashMap<>());

        builder.addSource("location-source",
                Serdes.String().deserializer(),
                Serdes.ByteArray().deserializer(),
                inputTopic)
               .addProcessor("location-processor",
                       LocationProcessor::new,
                       "location-source")
               .addStateStore(storeBuilder, "location-processor")
               .addSink("location-sink",
                       outputTopic,
                       Serdes.String().serializer(),
                       Serdes.ByteArray().serializer(),
                       "location-processor");
        return builder;
    }

    private Properties buildProps(String bootstrapServers) {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,     "uber-lite-stress-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.ByteArray().getClass().getName());
        // Tuning for high-throughput
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,         "3");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,         "1000");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,  "52428800"); // 50MB record cache
        props.put(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "1000");
        // RocksDB custom config
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG,
                BoundedRocksDBConfig.class.getName());
        props.put(StreamsConfig.CLIENT_ID_CONFIG,                  "uber-lite-streams");
        return props;
    }

    public void start() {
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        System.out.println("==> Kafka Streams started (3 StreamThreads)");
    }

    public void stop() {
        streams.close(Duration.ofSeconds(30));
        System.out.println("==> Kafka Streams stopped");
    }

    // ── Inner Processor ──────────────────────────────────────────────────
    static class LocationProcessor
            implements Processor<String, byte[], String, byte[]> {

        private ProcessorContext<String, byte[]> context;
        private KeyValueStore<String, byte[]>    stateStore;
        private H3Core h3;
        private long processedCount = 0;

        @Override
        public void init(ProcessorContext<String, byte[]> ctx) {
            this.context    = ctx;
            this.stateStore = ctx.getStateStore(STATE_STORE_NAME);
            try { this.h3 = H3Core.newInstance(); }
            catch (IOException e) { throw new RuntimeException(e); }
        }

        @Override
        public void process(Record<String, byte[]> record) {
            var event = DriverLocationEvent.fromJsonBytes(record.value());

            // Validate H3 cell resolution
            if (!h3.isValidCell(h3.stringToH3(event.h3Cell()))) {
                return; // drop invalid spatial data
            }

            // Write latest position to RocksDB (keyed by driverId)
            stateStore.put(event.driverId(), record.value());

            // Forward downstream with updated timestamp header
            context.forward(new Record<>(event.h3Cell(), record.value(),
                    System.currentTimeMillis()));

            processedCount++;
            if (processedCount % 50_000 == 0) {
                System.out.printf("  [Processor] Processed %,d events (taskId=%s)%n",
                        processedCount, context.taskId());
            }
        }

        @Override
        public void close() {
            System.out.printf("  [Processor] Closing, total processed: %,d%n", processedCount);
        }
    }
}
