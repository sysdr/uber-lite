package com.uberlite.locality.topology;

import com.uberlite.locality.model.DriverLocationEvent;
import com.uberlite.locality.model.MatchResult;
import com.uberlite.locality.model.RiderRequestEvent;
import com.uberlite.locality.serde.JsonSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data Locality Matching Topology
 *
 * SOURCE TOPICS (both with 12 partitions, H3GeoPartitioner):
 *   driver-locations : KTable<h3Cell, DriverLocationEvent>
 *   rider-requests   : KStream<h3Cell, RiderRequestEvent>
 *
 * CO-PARTITIONING PROOF:
 *   - Same key type (H3 cell hex string)
 *   - Same partition count (12)
 *   - Same partitioner (H3GeoPartitioner on both producers)
 *   → Kafka Streams InternalTopicManager: NO repartition topic created
 *   → TaskManager assigns partition N from BOTH topics to the same StreamThread
 *   → Join is a local RocksDB lookup — zero network I/O
 *
 * OUTPUT TOPIC:
 *   match-results : KStream<h3Cell, MatchResult>
 */
public final class MatchingTopology {

    private static final Logger LOG = LoggerFactory.getLogger(MatchingTopology.class);

    public static final String DRIVER_TOPIC  = "driver-locations";
    public static final String RIDER_TOPIC   = "rider-requests";
    public static final String MATCH_TOPIC   = "match-results";
    public static final String DRIVER_STORE  = "driver-location-store";

    public static Topology build() {
        var builder = new StreamsBuilder();

        var stringSerde      = Serdes.String();
        var driverEventSerde = new JsonSerde<>(DriverLocationEvent.class);
        var riderEventSerde  = new JsonSerde<>(RiderRequestEvent.class);
        var matchResultSerde = new JsonSerde<>(MatchResult.class);

        // ── KTable: Driver Locations ───────────────────────────────────────
        // Materialized as a RocksDB-backed state store named DRIVER_STORE.
        // Key: H3 cell hex string. Value: latest DriverLocationEvent for that cell.
        //
        // NOTE: This KTable stores ONE driver per H3 cell (the most recent update).
        // Production matching (multiple drivers per cell via composite key) is Lesson 27.
        // This lesson proves data locality — the join mechanics, not the match algorithm.
        var driverTable = builder.table(
            DRIVER_TOPIC,
            Consumed.with(stringSerde, driverEventSerde)
                    .withName("driver-location-source"),
            Materialized.<String, DriverLocationEvent, KeyValueStore<Bytes, byte[]>>as(DRIVER_STORE)
                        .withKeySerde(stringSerde)
                        .withValueSerde(driverEventSerde)
                        .withCachingEnabled()  // RocksDB write buffer — batches before flushing
        );

        // ── KStream: Rider Requests ────────────────────────────────────────
        // Key: H3 cell hex string (same scheme as driver KTable).
        // Co-partitioning guarantee: partition(h3Cell) maps to the same partition
        // on both topics, so this join requires ZERO repartitioning.
        var riderStream = builder.stream(
            RIDER_TOPIC,
            Consumed.with(stringSerde, riderEventSerde)
                    .withName("rider-request-source")
        );

        // ── KStream-KTable Join ────────────────────────────────────────────
        // This join is LOCAL because:
        //   1. Both streams keyed by same H3 cell
        //   2. Both topics have identical partition count (12)
        //   3. H3GeoPartitioner applied identically to both producers
        //
        // Kafka Streams internal flow:
        //   StreamThread-N reads rider-requests partition N
        //   → calls KTableValueGetter.get(h3CellKey)
        //   → RocksDB lookup in DRIVER_STORE, partition-N shard
        //   → returns DriverLocationEvent without any network call
        var matchStream = riderStream.join(
            driverTable,
            (riderRequest, driverEvent) -> {
                if (driverEvent == null) {
                    // No driver in this H3 cell — K-ring expansion in Lesson 27
                    LOG.debug("No driver in cell {} for rider {}", riderRequest.h3Cell(), riderRequest.riderId());
                    return null;
                }
                if (!"AVAILABLE".equals(driverEvent.status())) {
                    return null;  // Driver not available (ON_TRIP or OFFLINE)
                }
                long now = System.currentTimeMillis();
                long latency = now - riderRequest.timestampMs();
                LOG.info("MATCH: rider={} driver={} cell={} latency={}ms",
                    riderRequest.riderId(), driverEvent.driverId(),
                    riderRequest.h3Cell(), latency);
                // partition field captured from h3Cell routing — proves both were on same partition
                return new MatchResult(
                    riderRequest.riderId(),
                    driverEvent.driverId(),
                    riderRequest.h3Cell(),
                    -1,   // partition populated in LocalityMatchingApp interceptor
                    latency,
                    now
                );
            },
            Joined.with(stringSerde, riderEventSerde, driverEventSerde)
                  .withName("driver-rider-local-join")
        );

        // Filter null results (no driver available) and write matches
        matchStream
            .filter((key, value) -> value != null)
            .to(MATCH_TOPIC, Produced.with(stringSerde, matchResultSerde)
                                     .withName("match-results-sink"));

        var topology = builder.build();
        LOG.info("Topology built:\n{}", topology.describe());
        return topology;
    }
}
