package com.uberlite.boundary;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

/**
 * Builds the Kafka Streams Processor API topology for Lesson 34.
 *
 * Topology:
 *   [driver-location-raw] → BoundaryBufferingProcessor → [driver-location-partitioned]
 *                                                              ↓
 *                                                      DedupProcessor
 *                                                              ↓
 *                                                    [driver-location-deduped]
 */
public final class BoundaryBufferingTopology {

    public static final String SOURCE_TOPIC  = "driver-location-raw";
    public static final String MULTICAST_TOPIC = "driver-location-partitioned";
    public static final String DEDUPED_TOPIC = "driver-location-deduped";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Topology build(BoundaryDetector detector, BoundaryMetrics metrics) {
        var topology = new Topology();

        // --- Source ---
        topology.addSource(
                "DriverLocationSource",
                Serdes.String().deserializer(),
                new DriverLocationEventSerde().deserializer(),
                SOURCE_TOPIC
        );

        // --- Boundary Buffering Processor (stateless, multicast) ---
        topology.addProcessor(
                "BoundaryBufferingProcessor",
                () -> new BoundaryBufferingProcessor(detector, metrics),
                "DriverLocationSource"
        );

        // --- Sink: multicasted (may contain duplicates for boundary drivers) ---
        topology.addSink(
                "MulticastSink",
                MULTICAST_TOPIC,
                Serdes.String().serializer(),
                new DriverLocationEventSerde().serializer(),
                "BoundaryBufferingProcessor"
        );

        // --- Dedup Processor (stateful, RocksDB-backed) ---
        // Note: in a real production topology this would be a separate Streams app
        // consuming from driver-location-partitioned. Shown inline here for clarity.
        topology.addSource(
                "MulticastSource",
                Serdes.String().deserializer(),
                new DriverLocationEventSerde().deserializer(),
                MULTICAST_TOPIC
        );

        topology.addProcessor(
                "DedupProcessor",
                () -> new DedupProcessor(metrics),
                "MulticastSource"
        );

        topology.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(DedupProcessor.STORE_NAME),
                        Serdes.String(),
                        Serdes.Long()
                ),
                "DedupProcessor"
        );

        // --- Final sink: deduped, match-ready events ---
        topology.addSink(
                "DedupedSink",
                DEDUPED_TOPIC,
                Serdes.String().serializer(),
                new DriverLocationEventSerde().serializer(),
                "DedupProcessor"
        );

        return topology;
    }
}
