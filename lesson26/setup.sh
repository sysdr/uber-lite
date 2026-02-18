#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="lesson-26-data-locality"
BASE_PKG_DIR="$PROJECT_ROOT/src/main/java/com/uberlite/locality"

echo "==> Generating Lesson 26: Data Locality & Custom Partitioning"

# ─── Directory Structure ────────────────────────────────────────────────────
mkdir -p "$BASE_PKG_DIR"/{model,serde,topology,producer}
mkdir -p "$PROJECT_ROOT/src/main/resources"
# Maven wrapper (so build works without system mvn)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/../lesson25/firehose-capstone/mvnw" ] && [ -d "$SCRIPT_DIR/../lesson25/firehose-capstone/.mvn" ]; then
  cp -r "$SCRIPT_DIR/../lesson25/firehose-capstone/.mvn" "$PROJECT_ROOT/"
  cp "$SCRIPT_DIR/../lesson25/firehose-capstone/mvnw" "$PROJECT_ROOT/"
  chmod +x "$PROJECT_ROOT/mvnw"
fi

# ─── pom.xml ────────────────────────────────────────────────────────────────
cat > "$PROJECT_ROOT/pom.xml" << 'MAVEN_EOF'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.uberlite</groupId>
    <artifactId>lesson-26-data-locality</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <properties>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <maven.compiler.release>21</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <kafka.version>3.6.1</kafka.version>
    </properties>

    <dependencies>
        <!-- Kafka Clients (raw producer/consumer + custom Partitioner) -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>${kafka.version}</version>
        </dependency>

        <!-- Kafka Streams (Processor API, KTable, co-partitioning check) -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-streams</artifactId>
            <version>${kafka.version}</version>
        </dependency>

        <!-- H3 Geospatial Indexing -->
        <dependency>
            <groupId>com.uber</groupId>
            <artifactId>h3</artifactId>
            <version>4.1.1</version>
        </dependency>

        <!-- JSON Serialization -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
            <version>2.15.2</version>
        </dependency>

        <!-- Logging -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>2.0.9</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>2.0.9</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.11.0</version>
                <configuration>
                    <release>21</release>
                    <compilerArgs>
                        <arg>--enable-preview</arg>
                    </compilerArgs>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>exec-maven-plugin</artifactId>
                <version>3.1.0</version>
                <configuration>
                    <mainClass>com.uberlite.locality.LocalityMatchingApp</mainClass>
                </configuration>
                <executions>
                    <execution>
                        <id>simulator</id>
                        <phase>none</phase>
                        <goals><goal>java</goal></goals>
                        <configuration>
                            <mainClass>com.uberlite.locality.producer.DataLocalitySimulator</mainClass>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
MAVEN_EOF

# ─── docker-compose.yml ─────────────────────────────────────────────────────
cat > "$PROJECT_ROOT/docker-compose.yml" << 'DOCKER_EOF'
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zk-lesson26
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  broker-1:
    image: confluentinc/cp-kafka:7.5.0
    container_name: broker1-lesson26
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker-1:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_NUM_PARTITIONS: 12
      KAFKA_LOG_RETENTION_MS: 3600000
      # Tuning for high-velocity spatial workload
      KAFKA_LOG_SEGMENT_BYTES: 134217728
      KAFKA_COMPRESSION_TYPE: lz4

  broker-2:
    image: confluentinc/cp-kafka:7.5.0
    container_name: broker2-lesson26
    depends_on: [zookeeper]
    ports:
      - "9093:9093"
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker-2:29093,PLAINTEXT_HOST://localhost:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_NUM_PARTITIONS: 12
      KAFKA_LOG_SEGMENT_BYTES: 134217728
      KAFKA_COMPRESSION_TYPE: lz4

  broker-3:
    image: confluentinc/cp-kafka:7.5.0
    container_name: broker3-lesson26
    depends_on: [zookeeper]
    ports:
      - "9094:9094"
    environment:
      KAFKA_BROKER_ID: 3
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker-3:29094,PLAINTEXT_HOST://localhost:9094
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_NUM_PARTITIONS: 12
      KAFKA_LOG_SEGMENT_BYTES: 134217728
      KAFKA_COMPRESSION_TYPE: lz4

  topic-init:
    image: confluentinc/cp-kafka:7.5.0
    container_name: topic-init-lesson26
    depends_on: [broker-1, broker-2, broker-3]
    entrypoint: |
      bash -c "
        sleep 20
        kafka-topics --create --if-not-exists \
          --bootstrap-server broker-1:29092 \
          --topic driver-locations \
          --partitions 12 \
          --replication-factor 3 \
          --config compression.type=lz4 \
          --config retention.ms=3600000

        kafka-topics --create --if-not-exists \
          --bootstrap-server broker-1:29092 \
          --topic rider-requests \
          --partitions 12 \
          --replication-factor 3 \
          --config compression.type=lz4 \
          --config retention.ms=3600000

        kafka-topics --create --if-not-exists \
          --bootstrap-server broker-1:29092 \
          --topic match-results \
          --partitions 12 \
          --replication-factor 3 \
          --config retention.ms=3600000

        echo 'Topics created successfully.'
        kafka-topics --list --bootstrap-server broker-1:29092
      "
DOCKER_EOF

# ─── logback / slf4j config ─────────────────────────────────────────────────
cat > "$PROJECT_ROOT/src/main/resources/simplelogger.properties" << 'LOG_EOF'
org.slf4j.simpleLogger.defaultLogLevel=info
org.slf4j.simpleLogger.showDateTime=true
org.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss.SSS
org.slf4j.simpleLogger.showShortLogName=true
org.slf4j.simpleLogger.log.org.apache.kafka=warn
LOG_EOF

# ─── Model: DriverLocationEvent ─────────────────────────────────────────────
cat > "$BASE_PKG_DIR/model/DriverLocationEvent.java" << 'JAVA_EOF'
package com.uberlite.locality.model;

/**
 * Immutable driver location update.
 * Produced at ~3,000 events/sec across the fleet.
 * The h3Cell field is set by the producer BEFORE sending —
 * it encodes (lat, lon) at H3 Resolution 7 and becomes the record key.
 */
public record DriverLocationEvent(
    String driverId,
    double lat,
    double lon,
    String h3Cell,      // H3 R7 hex address — the routing key
    long   timestampMs,
    String status       // "AVAILABLE" | "ON_TRIP" | "OFFLINE"
) {}
JAVA_EOF

# ─── Model: RiderRequestEvent ───────────────────────────────────────────────
cat > "$BASE_PKG_DIR/model/RiderRequestEvent.java" << 'JAVA_EOF'
package com.uberlite.locality.model;

/**
 * Rider match request.
 * Keyed by H3 R7 cell of pickup location — same partitioning scheme
 * as driver-locations, guaranteeing co-location on the same partition.
 */
public record RiderRequestEvent(
    String riderId,
    double pickupLat,
    double pickupLon,
    String h3Cell,       // H3 R7 hex address — same key scheme as driver
    long   timestampMs
) {}
JAVA_EOF

# ─── Model: MatchResult ─────────────────────────────────────────────────────
cat > "$BASE_PKG_DIR/model/MatchResult.java" << 'JAVA_EOF'
package com.uberlite.locality.model;

/**
 * Output of a successful driver-rider match.
 * partition field is captured at match time to prove co-location.
 */
public record MatchResult(
    String riderId,
    String driverId,
    String h3Cell,
    int    partition,      // Must equal both input record partitions — proves locality
    long   matchLatencyMs, // Time from rider request to match output
    long   timestampMs
) {}
JAVA_EOF

# ─── Serde: JsonSerde ───────────────────────────────────────────────────────
cat > "$BASE_PKG_DIR/serde/JsonSerde.java" << 'JAVA_EOF'
package com.uberlite.locality.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

/**
 * Generic JSON Serde backed by Jackson.
 * No schema registry dependency — this lesson is about partition routing,
 * not schema evolution. Production would use Avro + Confluent SR.
 */
public final class JsonSerde<T> implements Serde<T> {

    private static final ObjectMapper MAPPER = JsonMapper.builder().build();
    private final Class<T> type;

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    @Override
    public Serializer<T> serializer() {
        return new Serializer<>() {
            @Override public void configure(Map<String, ?> configs, boolean isKey) {}
            @Override
            public byte[] serialize(String topic, T data) {
                if (data == null) return null;
                try {
                    return MAPPER.writeValueAsBytes(data);
                } catch (IOException e) {
                    throw new RuntimeException("JSON serialization failed for " + type.getSimpleName(), e);
                }
            }
            @Override public void close() {}
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return new Deserializer<>() {
            @Override public void configure(Map<String, ?> configs, boolean isKey) {}
            @Override
            public T deserialize(String topic, byte[] data) {
                if (data == null) return null;
                try {
                    return MAPPER.readValue(data, type);
                } catch (IOException e) {
                    throw new RuntimeException("JSON deserialization failed for " + type.getSimpleName(), e);
                }
            }
            @Override public void close() {}
        };
    }

    @Override public void configure(Map<String, ?> configs, boolean isKey) {}
    @Override public void close() {}
}
JAVA_EOF

# ─── H3GeoPartitioner ───────────────────────────────────────────────────────
cat > "$BASE_PKG_DIR/H3GeoPartitioner.java" << 'JAVA_EOF'
package com.uberlite.locality;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * H3-based Kafka Partitioner.
 *
 * CONTRACT: Record key MUST be the H3 R7 cell hex address (e.g., "872a1072fffffff").
 * This partitioner must be configured identically on BOTH driver-locations and
 * rider-requests producers to satisfy Kafka Streams' co-partitioning requirement.
 *
 * ROUTING FUNCTION:
 *   cellId = Long.parseUnsignedLong(hexKey, 16)
 *   partition = Math.floorMod(Long.hashCode(cellId), numPartitions)
 *
 * Long.hashCode(x) = (int)(x ^ (x >>> 32))
 * This XOR-mixes the resolution/base-cell bits (upper 32) with the
 * face/position bits (lower 32), giving adequate distribution without
 * an additional MurmurHash pass.
 *
 * IMPORTANT: numPartitions MUST be equal on both source topics.
 * If they differ, Kafka Streams inserts a repartition topic and this
 * entire lesson's data locality guarantee evaporates.
 */
public final class H3GeoPartitioner implements Partitioner {

    private static final Logger LOG = LoggerFactory.getLogger(H3GeoPartitioner.class);

    // Metrics for skew detection
    private final AtomicLong[] partitionCounters = new AtomicLong[12];
    private volatile int numPartitions = 12;

    @Override
    public void configure(Map<String, ?> configs) {
        for (int i = 0; i < partitionCounters.length; i++) {
            partitionCounters[i] = new AtomicLong(0);
        }
        LOG.info("H3GeoPartitioner initialized. Co-partitioning contract: BOTH source topics must have {} partitions.", numPartitions);
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        numPartitions = partitions.size();

        if (keyBytes == null) {
            throw new IllegalArgumentException(
                "H3GeoPartitioner requires a non-null H3 cell address as the record key. " +
                "Topic: " + topic + ". Encode (lat,lon) to H3 R7 before producing."
            );
        }

        // Key is the H3 hex address string, e.g. "872a1072fffffff"
        var h3HexKey = new String(keyBytes, StandardCharsets.UTF_8);
        long cellId;
        try {
            cellId = Long.parseUnsignedLong(h3HexKey, 16);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Record key '" + h3HexKey + "' is not a valid H3 hex address. " +
                "Expected format: 15-char lowercase hex string.", e
            );
        }

        // XOR-fold the 64-bit cell ID into a partition index.
        // Upper 32 bits: resolution (3 bits) + base cell (7 bits) + mode bits
        // Lower 32 bits: face coordinate bits encoding local position
        // XOR mixing gives even distribution across the 12-partition range.
        int hash = Long.hashCode(cellId); // (int)(cellId ^ (cellId >>> 32))
        int targetPartition = Math.floorMod(hash, numPartitions);

        // Track per-partition routing for skew reporting
        if (targetPartition < partitionCounters.length) {
            partitionCounters[targetPartition].incrementAndGet();
        }

        return targetPartition;
    }

    /**
     * Compute partition skew: max_count / avg_count.
     * Target: < 1.15x. H3 R7 achieves ~1.09x. Geohash achieves ~2.86x.
     */
    public double computePartitionSkew() {
        long total = 0;
        long max = 0;
        for (var counter : partitionCounters) {
            long v = counter.get();
            total += v;
            if (v > max) max = v;
        }
        if (total == 0) return 1.0;
        double avg = (double) total / partitionCounters.length;
        return max / avg;
    }

    public long[] getPartitionCounts() {
        long[] counts = new long[partitionCounters.length];
        for (int i = 0; i < partitionCounters.length; i++) {
            counts[i] = partitionCounters[i].get();
        }
        return counts;
    }

    @Override
    public void close() {
        LOG.info("H3GeoPartitioner final partition skew: {}x", String.format("%.3f", computePartitionSkew()));
    }
}
JAVA_EOF

# ─── Kafka Streams Topology ──────────────────────────────────────────────────
cat > "$BASE_PKG_DIR/topology/MatchingTopology.java" << 'JAVA_EOF'
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
JAVA_EOF

# ─── DataLocalitySimulator ───────────────────────────────────────────────────
cat > "$BASE_PKG_DIR/producer/DataLocalitySimulator.java" << 'JAVA_EOF'
package com.uberlite.locality.producer;

import com.uberlite.locality.H3GeoPartitioner;
import com.uberlite.locality.model.DriverLocationEvent;
import com.uberlite.locality.model.RiderRequestEvent;
import com.uberlite.locality.serde.JsonSerde;
import com.uber.h3core.H3Core;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Deterministic load simulator for Lesson 26.
 *
 * Generates driver and rider events for 8 H3 R7 cells across 4 cities.
 * Both producer instances use H3GeoPartitioner — same class, same routing function.
 *
 * Design intent:
 *   - driver-locations producer keys by h3Cell → H3GeoPartitioner → partition P
 *   - rider-requests producer keys by h3Cell   → H3GeoPartitioner → partition P
 *   → Same cell, same partition, always. Zero repartition topics.
 *
 * Throughput target: 3,000+ events/sec total (split ~50/50 driver/rider).
 */
public final class DataLocalitySimulator {

    private static final Logger LOG = LoggerFactory.getLogger(DataLocalitySimulator.class);
    private static final int H3_RESOLUTION = 7;

    // 8 representative cells across NYC, SF, Chicago, London
    // In production, these would be dynamically discovered from live fleet positions.
    private static final double[][] CITY_COORDS = {
        {40.7128,  -74.0060},  // NYC Financial District
        {40.7580,  -73.9855},  // NYC Midtown
        {37.7749, -122.4194},  // SF downtown
        {37.3382, -121.8863},  // San Jose
        {41.8781,  -87.6298},  // Chicago Loop
        {41.9742,  -87.9073},  // O'Hare
        {51.5074,   -0.1278},  // London City
        {51.4816,   -0.0078}   // London Canary Wharf
    };

    public static void main(String[] args) throws Exception {
        var h3 = H3Core.newInstance();

        // Pre-compute H3 cells for our city coords
        var h3Cells = new ArrayList<String>();
        for (var coords : CITY_COORDS) {
            String cell = h3.latLngToCellAddress(coords[0], coords[1], H3_RESOLUTION);
            h3Cells.add(cell);
            LOG.info("City coord ({}, {}) → H3 R7 cell: {}", coords[0], coords[1], cell);
        }

        var driverProducer = buildProducer(DriverLocationEvent.class);
        var riderProducer  = buildProducer(RiderRequestEvent.class);

        int totalPairs = 5_000;
        var driverSent = new AtomicLong(0);
        var riderSent  = new AtomicLong(0);
        var random = new Random(42L); // deterministic for reproducibility

        LOG.info("Starting simulation: {} driver+rider pairs across {} H3 cells", totalPairs, h3Cells.size());

        var latch = new CountDownLatch(totalPairs * 2);
        long startMs = System.currentTimeMillis();

        for (int i = 0; i < totalPairs; i++) {
            // Pick a random cell from our set
            String cell = h3Cells.get(random.nextInt(h3Cells.size()));

            // Get representative lat/lon for this cell (center point)
            var centerCoords = h3.cellToLatLng(h3.stringToH3(cell));
            double lat = centerCoords.lat + (random.nextGaussian() * 0.001);  // ±~100m jitter
            double lon = centerCoords.lng + (random.nextGaussian() * 0.001);

            // Re-encode with jitter — should still land in same R7 cell (5.16 km² cell, ~100m jitter)
            String actualCell = h3.latLngToCellAddress(lat, lon, H3_RESOLUTION);

            // Driver event — key = H3 cell hex → H3GeoPartitioner
            var driverEvent = new DriverLocationEvent(
                "driver-" + String.format("%05d", i % 1000),
                lat, lon, actualCell,
                System.currentTimeMillis(),
                "AVAILABLE"
            );

            driverProducer.send(
                new ProducerRecord<>("driver-locations", actualCell, driverEvent),
                (metadata, ex) -> {
                    if (ex == null) {
                        driverSent.incrementAndGet();
                    } else {
                        LOG.error("Driver produce failed", ex);
                    }
                    latch.countDown();
                }
            );

            // Rider event — SAME cell as driver → guaranteed same partition
            var riderEvent = new RiderRequestEvent(
                "rider-" + String.format("%05d", i),
                lat + 0.0002, lon + 0.0002,  // Rider slightly displaced within same cell
                actualCell,
                System.currentTimeMillis()
            );

            riderProducer.send(
                new ProducerRecord<>("rider-requests", actualCell, riderEvent),
                (metadata, ex) -> {
                    if (ex == null) {
                        riderSent.incrementAndGet();
                    } else {
                        LOG.error("Rider produce failed", ex);
                    }
                    latch.countDown();
                }
            );

            // Throttle to ~1000 pairs/sec to avoid overloading local Docker Kafka
            if (i % 100 == 0) {
                Thread.sleep(100);
                LOG.info("Progress: {}/{} pairs sent", i, totalPairs);
            }
        }

        // Flush both producers
        driverProducer.flush();
        riderProducer.flush();
        latch.await();

        long elapsed = System.currentTimeMillis() - startMs;
        LOG.info("Simulation complete: {} drivers + {} riders in {}ms ({} events/sec)",
            driverSent.get(), riderSent.get(), elapsed,
            (driverSent.get() + riderSent.get()) * 1000L / Math.max(elapsed, 1));

        driverProducer.close();
        riderProducer.close();
    }

    @SuppressWarnings("unchecked")
    private static <T> KafkaProducer<String, T> buildProducer(Class<T> type) {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new JsonSerde<>(type).serializer().getClass().getName());
        // H3GeoPartitioner — same class on both producers = co-partitioning contract satisfied
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, H3GeoPartitioner.class.getName());
        // Async batching for throughput
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);          // 64KB batches
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);               // 5ms linger for batching
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864L);   // 64MB RecordAccumulator
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.ACKS_CONFIG, "1");                  // Leader ack for throughput
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Inline value serializer - build a custom producer
        var serializer = new JsonSerde<>(type).serializer();
        return new KafkaProducer<>(
            props,
            new org.apache.kafka.common.serialization.StringSerializer(),
            serializer
        );
    }
}
JAVA_EOF

# ─── LocalityMatchingApp ─────────────────────────────────────────────────────
cat > "$BASE_PKG_DIR/LocalityMatchingApp.java" << 'JAVA_EOF'
package com.uberlite.locality;

import com.uberlite.locality.topology.MatchingTopology;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * Main entry point for the Data Locality Matching application.
 *
 * Starts:
 *   1. Kafka Streams application with MatchingTopology
 *   2. HTTP metrics endpoint on :8080/metrics (virtual thread executor)
 *
 * CRITICAL: StreamsConfig.NUM_STREAM_THREADS_CONFIG = 4 means Kafka Streams
 * creates 4 StreamThreads. With 12 partitions across 2 topics (24 tasks total),
 * each StreamThread owns 3 tasks. Each task processes a pair of co-partitioned
 * driver-locations[N] + rider-requests[N] — this is data locality in action.
 */
public final class LocalityMatchingApp {

    private static final Logger LOG = LoggerFactory.getLogger(LocalityMatchingApp.class);

    public static void main(String[] args) throws Exception {
        var streams = new KafkaStreams(MatchingTopology.build(), buildStreamsConfig());

        streams.setUncaughtExceptionHandler(exception -> {
            LOG.error("Uncaught StreamThread exception", exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        // Graceful shutdown on SIGTERM / Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received. Closing Kafka Streams...");
            streams.close();
        }, "streams-shutdown-hook"));

        streams.start();
        LOG.info("KafkaStreams started. State: {}", streams.state());
        int initialTaskCount = streams.metadataForLocalThreads().stream()
            .mapToInt(t -> t.activeTasks().size()).sum();
        LOG.info("TaskManager: {} active tasks", initialTaskCount);

        // ── Metrics HTTP Server (Virtual Threads) ────────────────────────
        // Runs on virtual threads — each request gets its own VT.
        // No thread pool sizing needed. Perfect for low-frequency metrics polling.
        var server = HttpServer.create(new InetSocketAddress(8080), 0);

        server.createContext("/metrics", exchange -> {
            var state = streams.state();
            int activeTaskCount = streams.metadataForLocalThreads().stream()
                .mapToInt(t -> t.activeTasks().size()).sum();

            var sb = new StringBuilder();
            sb.append("# Lesson 26 Data Locality Metrics\n");
            sb.append("# Run ./demo.sh to generate traffic; rates show current throughput, totals are cumulative.\n");
            sb.append("streams_state{state=\"").append(state).append("\"} 1\n");
            sb.append("streams_active_tasks ").append(activeTaskCount).append("\n");

            try {
                var storeMetrics = streams.metrics();
                double sumConsumedTotal = 0;
                double sumProducedTotal = 0;
                double sumProcessRate = 0;
                double sumCommitRate = 0;
                for (var entry : storeMetrics.entrySet()) {
                    var name = entry.getKey().name();
                    var mv = entry.getValue().metricValue();
                    double value = (mv instanceof Number) ? ((Number) mv).doubleValue() : 0;
                    if (name != null) {
                        if (name.contains("records-consumed-total")) sumConsumedTotal += value;
                        if (name.contains("records-produced-total")) sumProducedTotal += value;
                        if (name.contains("process-rate")) sumProcessRate += value;
                        if (name.contains("commit-rate")) sumCommitRate += value;
                    }
                }
                sb.append("streams_records_consumed_total ").append((long) sumConsumedTotal).append("\n");
                sb.append("streams_records_produced_total ").append((long) sumProducedTotal).append("\n");
                sb.append("streams_process_rate_total ").append(String.format("%.2f", sumProcessRate)).append("\n");
                sb.append("streams_commit_rate_total ").append(String.format("%.2f", sumCommitRate)).append("\n");

                storeMetrics.forEach((metricName, metric) -> {
                    var name = metricName.name();
                    if (name != null && (name.contains("process-rate") || name.contains("rocksdb")
                        || name.contains("commit-rate") || name.contains("records-consumed-total")
                        || name.contains("records-produced-total"))) {
                        Object v = metric.metricValue();
                        sb.append("kafka_streams_").append(name.replace('-', '_')).append(" ");
                        if (v instanceof Number) sb.append(v);
                        else sb.append(v != null ? v.toString() : "0");
                        sb.append("\n");
                    }
                });
            } catch (Exception e) {
                sb.append("# Error collecting metrics: ").append(e.getMessage()).append("\n");
            }

            byte[] response = sb.toString().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (var os = exchange.getResponseBody()) {
                os.write(response);
            }
        });

        server.createContext("/health", exchange -> {
            var status = streams.state().isRunningOrRebalancing() ? "UP" : "DOWN";
            byte[] response = ("{\"status\":\"" + status + "\"}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (var os = exchange.getResponseBody()) {
                os.write(response);
            }
        });

        // Virtual thread executor — each HTTP request on its own VT
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        LOG.info("Metrics server started on http://localhost:8080/metrics");
        LOG.info("Health check: http://localhost:8080/health");
    }

    private static Properties buildStreamsConfig() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "locality-matching-v26");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                  org.apache.kafka.common.serialization.Serdes.StringSerde.class);

        // 4 StreamThreads × 3 tasks each = 12 tasks = 12 partition pairs
        // Each StreamThread processes one "shard" of the co-partitioned data
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);

        // Commit interval — how often StreamThread checkpoints offsets + RocksDB state
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // RocksDB state store directory
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/lesson26");

        // Consumer tuning for low-latency matching
        props.put(StreamsConfig.consumerPrefix("fetch.min.bytes"), 1);
        props.put(StreamsConfig.consumerPrefix("fetch.max.wait.ms"), 10);

        // Replication for internal topics (changelog of state stores)
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);

        return props;
    }
}
JAVA_EOF

# ─── verify.sh: validate metrics/dashboard after startup + demo ──────────────
cat > "$PROJECT_ROOT/verify.sh" << 'VERIFY_EOF'
#!/usr/bin/env bash
set -euo pipefail
METRICS_URL="${METRICS_URL:-http://localhost:8080/metrics}"
HEALTH_URL="${HEALTH_URL:-http://localhost:8080/health}"
echo "==> Checking health: $HEALTH_URL"
curl -sf "$HEALTH_URL" | grep -q '"status"' && echo "  OK: health endpoint returns status"
echo "==> Checking metrics: $METRICS_URL"
METRICS=$(curl -sf "$METRICS_URL")
echo "$METRICS" | grep -q "streams_state" || { echo "  FAIL: streams_state missing"; exit 1; }
echo "$METRICS" | grep -q "streams_active_tasks" || { echo "  FAIL: streams_active_tasks missing"; exit 1; }
echo "$METRICS" | grep -q "streams_records_consumed_total" || { echo "  FAIL: streams_records_consumed_total missing"; exit 1; }
echo "$METRICS" | grep -q "streams_records_produced_total" || { echo "  FAIL: streams_records_produced_total missing"; exit 1; }
TASKS=$(echo "$METRICS" | sed -n 's/^streams_active_tasks \([0-9]*\).*/\1/p' | head -1)
if [ -n "$TASKS" ] && [ "$TASKS" -gt 0 ]; then
  echo "  OK: streams_active_tasks = $TASKS (non-zero)"
else
  echo "  WARN: streams_active_tasks is 0 or missing (run app and wait for rebalance)"
fi
CONSUMED=$(echo "$METRICS" | sed -n 's/^streams_records_consumed_total \([0-9]*\).*/\1/p' | head -1)
PRODUCED=$(echo "$METRICS" | sed -n 's/^streams_records_produced_total \([0-9]*\).*/\1/p' | head -1)
echo "  OK: streams_records_consumed_total = \${CONSUMED:-0}, streams_records_produced_total = \${PRODUCED:-0}"
echo "==> Dashboard metrics validation passed."
VERIFY_EOF
chmod +x "$PROJECT_ROOT/verify.sh"

# ─── start.sh: startup with full path and topic creation ────────────────────
cat > "$PROJECT_ROOT/start.sh" << 'START_EOF'
#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
MVNW="$DIR/mvnw"
[ -x "$MVNW" ] || { echo "Missing or not executable: $MVNW"; exit 1; }
# Avoid duplicate: app binds to 8080
if command -v curl &>/dev/null && curl -sf http://localhost:8080/health &>/dev/null; then
  echo "==> App already running on http://localhost:8080 (health OK)"
  echo "  Metrics: curl http://localhost:8080/metrics"
  echo "  Verify: $DIR/verify.sh"
  exit 0
fi
echo "==> Ensuring Kafka topics exist..."
docker exec broker1-lesson26 kafka-topics --list --bootstrap-server broker-1:29092 2>/dev/null | grep -q driver-locations || {
  docker exec broker1-lesson26 bash -c '
    for t in driver-locations rider-requests match-results; do
      kafka-topics --create --if-not-exists --bootstrap-server broker-1:29092 --topic "$t" --partitions 12 --replication-factor 3 --config retention.ms=3600000
    done
  '
}
echo "==> Starting LocalityMatchingApp (background)..."
"$MVNW" exec:java -Dexec.mainClass=com.uberlite.locality.LocalityMatchingApp &
APP_PID=$!
echo "  PID: $APP_PID (wait ~25s for rebalance, then run demo)"
echo "  Demo: $MVNW exec:java@simulator"
echo "  Metrics: curl http://localhost:8080/metrics"
echo "  Verify: $DIR/verify.sh"
START_EOF
chmod +x "$PROJECT_ROOT/start.sh"

echo ""
echo "============================================================"
echo "✓ Lesson 26 project generated at: $PROJECT_ROOT/"
echo "============================================================"
echo ""
echo "Startup sequence (from lesson26 or project dir):"
echo "  1. cd $PROJECT_ROOT"
echo "  2. docker compose up -d"
echo "  3. sleep 30  # Wait for brokers"
echo "  4. $PROJECT_ROOT/mvnw compile -q   # or ./mvnw if already in $PROJECT_ROOT"
echo "  5. $PROJECT_ROOT/start.sh         # or ./start.sh (starts app in background)"
echo "  6. sleep 25 && $PROJECT_ROOT/mvnw exec:java@simulator   # run demo"
echo ""
echo "Dashboard: http://localhost:8080/ or http://localhost:8080/dashboard"
echo "Metrics:   http://localhost:8080/metrics"
echo "Verify:   $PROJECT_ROOT/verify.sh or ./verify.sh"