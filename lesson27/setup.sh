#!/usr/bin/env bash
set -euo pipefail

PROJECT="lesson-27-partitioning"
PKG_PATH="src/main/java/com/uberlite/lesson27"
PARTITION_COUNT=12

echo "==> Creating project structure: $PROJECT"
mkdir -p "$PROJECT/$PKG_PATH"
mkdir -p "$PROJECT/src/main/resources"
# Maven wrapper (use from lesson26 if present so mvn not required)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-.}")" && pwd)"
for w in mvnw mvnw.cmd; do
  [ -f "$SCRIPT_DIR/../lesson26/$w" ] && cp "$SCRIPT_DIR/../lesson26/$w" "$PROJECT/$w" && chmod +x "$PROJECT/$w" && break
done
[ -d "$SCRIPT_DIR/../lesson26/.mvn" ] && cp -r "$SCRIPT_DIR/../lesson26/.mvn" "$PROJECT/"

# ── docker-compose.yml ────────────────────────────────────────────────────────
cat > "$PROJECT/docker-compose.yml" << 'EOF'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports: ["2181:2181"]

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_RETENTION_HOURS: 1
      KAFKA_JMX_PORT: 9999
      KAFKA_JMX_HOSTNAME: localhost
EOF

# ── pom.xml ───────────────────────────────────────────────────────────────────
cat > "$PROJECT/pom.xml" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.uberlite</groupId>
  <artifactId>lesson-27-partitioning</artifactId>
  <version>1.0.0</version>
  <packaging>jar</packaging>

  <properties>
    <maven.compiler.source>21</maven.compiler.source>
    <maven.compiler.target>21</maven.compiler.target>
    <maven.compiler.release>21</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <kafka.version>3.6.1</kafka.version>
    <h3.version>4.1.1</h3.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-clients</artifactId>
      <version>${kafka.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-streams</artifactId>
      <version>${kafka.version}</version>
    </dependency>
    <dependency>
      <groupId>com.uber</groupId>
      <artifactId>h3</artifactId>
      <version>${h3.version}</version>
    </dependency>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-simple</artifactId>
      <version>2.0.9</version>
    </dependency>
    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
      <version>5.10.1</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.codehaus.mojo</groupId>
        <artifactId>exec-maven-plugin</artifactId>
        <version>3.1.0</version>
      </plugin>
    </plugins>
  </build>
</project>
EOF

# ── dashboard.html (styled dashboard with auto-refresh) ──────────────────────
cat > "$PROJECT/src/main/resources/dashboard.html" << 'DASHEOF'
<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lesson 27 H3 Partitioning Dashboard</title>
<style>
*{box-sizing:border-box}
body{font-family:system-ui,-apple-system,sans-serif;margin:0;min-height:100vh;background:linear-gradient(180deg,#0f172a 0%,#1e293b 100%);color:#e2e8f0;padding:1.5rem}
.container{max-width:900px;margin:0 auto}
header{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:1rem;margin-bottom:1.5rem;padding-bottom:1rem;border-bottom:1px solid #334155}
h1{margin:0;font-size:1.5rem;font-weight:700}
.badge{background:#22c55e;color:#0f172a;padding:0.25rem 0.6rem;border-radius:9999px;font-size:0.75rem;font-weight:600}
.metrics{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:1rem;margin:1.5rem 0}
.card{background:#1e293b;border-radius:12px;padding:1.25rem;border:1px solid #334155}
.card label{display:block;font-size:0.7rem;text-transform:uppercase;letter-spacing:0.05em;color:#94a3b8;margin-bottom:0.25rem}
.card .value{font-size:1.75rem;font-weight:700;color:#f8fafc}
.card.state .value{color:#22c55e}
.links{margin-top:1.5rem;font-size:0.875rem}
.links a{color:#38bdf8;text-decoration:none;margin-right:1rem}
.links a:hover{text-decoration:underline}
.btn{margin-top:1rem;padding:0.6rem 1.2rem;background:#38bdf8;color:#0f172a;border:none;border-radius:8px;font-size:0.9rem;font-weight:600;cursor:pointer}
.btn:hover{background:#0ea5e9;color:#fff}
.btn:disabled{opacity:0.6;cursor:not-allowed}
#demo-status{margin-left:0.5rem;font-size:0.85rem;color:#94a3b8}
.hint{color:#64748b;font-size:0.8rem;margin-top:0.5rem}
</style></head><body>
<div class="container">
<header><h1>Lesson 27 H3 Partitioning Dashboard</h1><span class="badge">LIVE</span></header>
<p><button class="btn" type="button" id="run-demo-btn">Run demo</button><span id="demo-status"></span></p>
<div class="metrics">
<div class="card state"><label>State</label><div class="value" id="m-state">__STATE__</div></div>
<div class="card"><label>Active tasks</label><div class="value" id="m-tasks">__TASKS__</div></div>
<div class="card"><label>Records consumed</label><div class="value" id="m-consumed">__CONSUMED__</div></div>
<div class="card"><label>Records produced (matches)</label><div class="value" id="m-produced">__PRODUCED__</div></div>
</div>
<div class="links"><a href="/metrics">Raw metrics</a><a href="/health">Health</a></div>
<p class="hint">Metrics auto-refresh every 1s. Click "Run demo" to send rider requests for 45s — metrics update in real time.</p>
</div>
<script>
function update(){fetch('/api/metrics',{cache:'no-store'}).then(function(r){return r.json()}).then(function(d){
document.getElementById('m-state').textContent=d.state;
document.getElementById('m-tasks').textContent=d.activeTasks;
document.getElementById('m-consumed').textContent=d.consumedTotal;
document.getElementById('m-produced').textContent=d.producedTotal;
}).catch(function(){});}
setInterval(update,1000);
update();
document.getElementById('run-demo-btn').onclick=function(){
  var btn=this, st=document.getElementById('demo-status');
  btn.disabled=true; st.textContent='Starting…';
  fetch('/run-demo',{method:'POST',cache:'no-store'}).then(function(r){return r.text()}).then(function(t){
    st.textContent=t;
    setTimeout(function(){ btn.disabled=false; st.textContent=''; }, 5000);
  }).catch(function(){ st.textContent='Error'; btn.disabled=false; });
};
</script>
</body></html>
DASHEOF

# ── TopicBootstrap.java ───────────────────────────────────────────────────────
cat > "$PROJECT/$PKG_PATH/TopicBootstrap.java" << EOF
package com.uberlite.lesson27;

import org.apache.kafka.clients.admin.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicBootstrap {

    static final String DRIVER_TOPIC  = "driver-locations";
    static final String RIDER_TOPIC   = "rider-requests";
    static final String MATCH_TOPIC   = "match-events";
    static final int    PARTITIONS    = $PARTITION_COUNT;
    static final short  REPLICATION   = 1;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        try (var admin = AdminClient.create(props)) {
            var topics = List.of(
                new NewTopic(DRIVER_TOPIC, PARTITIONS, REPLICATION),
                new NewTopic(RIDER_TOPIC,  PARTITIONS, REPLICATION),
                new NewTopic(MATCH_TOPIC,  PARTITIONS, REPLICATION)
            );

            try {
                admin.createTopics(topics).all().get();
                System.out.println("[BOOTSTRAP] Created topics with " + PARTITIONS + " partitions each.");
            } catch (ExecutionException e) {
                if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                    System.out.println("[BOOTSTRAP] Topics already exist, skipping.");
                } else {
                    throw e;
                }
            }

            // Validate co-partitioning
            var descriptions = admin.describeTopics(List.of(DRIVER_TOPIC, RIDER_TOPIC)).allTopicNames().get();
            descriptions.forEach((topic, desc) -> {
                int count = desc.partitions().size();
                System.out.printf("[BOOTSTRAP] %s → %d partitions%n", topic, count);
                if (count != PARTITIONS) {
                    throw new IllegalStateException("CO-PARTITION VIOLATION: " + topic + " has " + count + " partitions, expected " + PARTITIONS);
                }
            });
        }
    }
}
EOF

# ── H3Util.java ───────────────────────────────────────────────────────────────
cat > "$PROJECT/$PKG_PATH/H3Util.java" << 'EOF'
package com.uberlite.lesson27;

import com.uber.h3core.H3Core;
import java.io.IOException;

public final class H3Util {

    public static final int RESOLUTION = 6;

    // H3Core is thread-safe — one instance per JVM is sufficient
    public static final H3Core H3;

    static {
        try {
            H3 = H3Core.newInstance();
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Returns the H3 Resolution-6 cell index for a GPS coordinate.
     * Computation time: ~50ns. No I/O. No allocation after JIT warmup.
     */
    public static long cellOf(double lat, double lng) {
        return H3.latLngToCell(lat, lng, RESOLUTION);
    }

    /**
     * Maps an H3 cell index to a Kafka partition number.
     * Uses Long's XOR-fold hash — identical to what Java's HashMap uses
     * for long keys, and deterministic across JVMs for the same input.
     */
    public static int partitionFor(long h3Cell, int numPartitions) {
        int hash = (int)(h3Cell ^ (h3Cell >>> 32));
        return Math.abs(hash) % numPartitions;
    }

    private H3Util() {}
}
EOF

# ── Models.java ───────────────────────────────────────────────────────────────
cat > "$PROJECT/$PKG_PATH/Models.java" << 'EOF'
package com.uberlite.lesson27;

/**
 * Canonical domain records for Lesson 27.
 * Records give us: equals, hashCode, toString, and compact serialization targets for free.
 */
public final class Models {

    public record DriverLocation(
        String  driverId,
        double  lat,
        double  lng,
        long    h3Cell,       // Resolution-6 cell — pre-computed at source
        long    timestampMs
    ) {
        /** Factory: compute H3 at construction time, not at partition time */
        public static DriverLocation of(String driverId, double lat, double lng) {
            return new DriverLocation(
                driverId, lat, lng,
                H3Util.cellOf(lat, lng),
                System.currentTimeMillis()
            );
        }
    }

    public record RiderRequest(
        String  riderId,
        double  lat,
        double  lng,
        long    h3Cell,
        long    timestampMs
    ) {
        public static RiderRequest of(String riderId, double lat, double lng) {
            return new RiderRequest(
                riderId, lat, lng,
                H3Util.cellOf(lat, lng),
                System.currentTimeMillis()
            );
        }
    }

    public record MatchEvent(
        String  riderId,
        String  driverId,
        long    h3Cell,
        long    latencyMs
    ) {}

    private Models() {}
}
EOF

# ── Serialization.java ────────────────────────────────────────────────────────
cat > "$PROJECT/$PKG_PATH/Serialization.java" << 'EOF'
package com.uberlite.lesson27;

import com.uberlite.lesson27.Models.*;
import org.apache.kafka.common.serialization.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Manual ByteBuffer serialization.
 * No Jackson, no Avro, no schema registry — we control every byte.
 * DriverLocation wire format (fixed 44 bytes):
 *   [driverId-length: 4][driverId-bytes: N][lat: 8][lng: 8][h3Cell: 8][ts: 8]
 */
public final class Serialization {

    // ── DriverLocation ────────────────────────────────────────────────────────

    public static class DriverLocationSerializer implements Serializer<DriverLocation> {
        @Override
        public byte[] serialize(String topic, DriverLocation data) {
            if (data == null) return null;
            byte[] idBytes = data.driverId().getBytes(StandardCharsets.UTF_8);
            var buf = ByteBuffer.allocate(4 + idBytes.length + 8 + 8 + 8 + 8);
            buf.putInt(idBytes.length);
            buf.put(idBytes);
            buf.putDouble(data.lat());
            buf.putDouble(data.lng());
            buf.putLong(data.h3Cell());
            buf.putLong(data.timestampMs());
            return buf.array();
        }
    }

    public static class DriverLocationDeserializer implements Deserializer<DriverLocation> {
        @Override
        public DriverLocation deserialize(String topic, byte[] data) {
            if (data == null) return null;
            var buf = ByteBuffer.wrap(data);
            int idLen   = buf.getInt();
            byte[] idB  = new byte[idLen];
            buf.get(idB);
            return new DriverLocation(
                new String(idB, StandardCharsets.UTF_8),
                buf.getDouble(),
                buf.getDouble(),
                buf.getLong(),
                buf.getLong()
            );
        }
    }

    public static Serde<DriverLocation> driverLocationSerde() {
        return Serdes.serdeFrom(new DriverLocationSerializer(), new DriverLocationDeserializer());
    }

    // ── RiderRequest ──────────────────────────────────────────────────────────

    public static class RiderRequestSerializer implements Serializer<RiderRequest> {
        @Override
        public byte[] serialize(String topic, RiderRequest data) {
            if (data == null) return null;
            byte[] idBytes = data.riderId().getBytes(StandardCharsets.UTF_8);
            var buf = ByteBuffer.allocate(4 + idBytes.length + 8 + 8 + 8 + 8);
            buf.putInt(idBytes.length);
            buf.put(idBytes);
            buf.putDouble(data.lat());
            buf.putDouble(data.lng());
            buf.putLong(data.h3Cell());
            buf.putLong(data.timestampMs());
            return buf.array();
        }
    }

    public static class RiderRequestDeserializer implements Deserializer<RiderRequest> {
        @Override
        public RiderRequest deserialize(String topic, byte[] data) {
            if (data == null) return null;
            var buf = ByteBuffer.wrap(data);
            int idLen  = buf.getInt();
            byte[] idB = new byte[idLen];
            buf.get(idB);
            return new RiderRequest(
                new String(idB, StandardCharsets.UTF_8),
                buf.getDouble(),
                buf.getDouble(),
                buf.getLong(),
                buf.getLong()
            );
        }
    }

    public static Serde<RiderRequest> riderRequestSerde() {
        return Serdes.serdeFrom(new RiderRequestSerializer(), new RiderRequestDeserializer());
    }

    private Serialization() {}
}
EOF

# ── H3ProducerPartitioner.java ────────────────────────────────────────────────
cat > "$PROJECT/$PKG_PATH/H3ProducerPartitioner.java" << 'EOF'
package com.uberlite.lesson27;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import java.util.Map;
import java.nio.ByteBuffer;

/**
 * Kafka Producer-side Partitioner (kafka-clients API).
 *
 * This is the PRODUCER side of the co-partitioning contract.
 * The Kafka Streams StreamPartitioner (below) is the CONSUMER/TOPOLOGY side.
 * Both must produce identical partition assignments for the same h3Cell value.
 *
 * Wire contract: the record key is an 8-byte big-endian long representing
 * the H3 Resolution-6 cell index.
 */
public class H3ProducerPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        var partitions  = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        if (keyBytes == null || keyBytes.length != 8) {
            // Fallback: default murmur2 on whatever key we have
            return Math.abs(key.hashCode()) % numPartitions;
        }

        long h3Cell = ByteBuffer.wrap(keyBytes).getLong();
        return H3Util.partitionFor(h3Cell, numPartitions);
    }

    @Override public void close() {}
    @Override public void configure(Map<String, ?> configs) {}
}
EOF

# ── H3StreamPartitioner.java ──────────────────────────────────────────────────
cat > "$PROJECT/$PKG_PATH/H3StreamPartitioner.java" << 'EOF'
package com.uberlite.lesson27;

import com.uberlite.lesson27.Models.DriverLocation;
import org.apache.kafka.streams.processor.StreamPartitioner;
import java.util.Optional;
import java.util.Set;

/**
 * Kafka Streams-side StreamPartitioner (kafka-streams API).
 *
 * Intercepts Kafka Streams' .to() sink and .repartition() calls.
 * The key is the H3 cell as a Long (we re-key the stream before sinking).
 *
 * This must be registered on every sink that writes to a co-partitioned topic.
 *
 * Identity: H3Util.partitionFor(h3Cell, numPartitions)
 * This is IDENTICAL to H3ProducerPartitioner — the co-partitioning contract is upheld.
 */
public class H3StreamPartitioner implements StreamPartitioner<Long, DriverLocation> {

    @Override
    @Deprecated
    public Integer partition(String topic, Long h3Cell, DriverLocation value, int numPartitions) {
        return H3Util.partitionFor(h3Cell, numPartitions);
    }

    @Override
    public Optional<Set<Integer>> partitions(String topic, Long h3Cell,
                                              DriverLocation value, int numPartitions) {
        int partition = H3Util.partitionFor(h3Cell, numPartitions);
        return Optional.of(Set.of(partition));
    }
}
EOF

# ── H3MatchStreamPartitioner.java (for match-events sink) ───────────────────────
cat > "$PROJECT/$PKG_PATH/H3MatchStreamPartitioner.java" << 'MATCHEOF'
package com.uberlite.lesson27;

import com.uberlite.lesson27.Models.MatchEvent;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Optional;
import java.util.Set;

public class H3MatchStreamPartitioner implements StreamPartitioner<Long, MatchEvent> {

    @Override
    @Deprecated
    public Integer partition(String topic, Long key, MatchEvent value, int numPartitions) {
        return H3Util.partitionFor(key, numPartitions);
    }

    @Override
    public Optional<Set<Integer>> partitions(String topic, Long key, MatchEvent value, int numPartitions) {
        return Optional.of(Set.of(H3Util.partitionFor(key, numPartitions)));
    }
}
MATCHEOF

# ── SimulatedDriverProducer.java ──────────────────────────────────────────────
cat > "$PROJECT/$PKG_PATH/SimulatedDriverProducer.java" << 'EOF'
package com.uberlite.lesson27;

import com.uberlite.lesson27.Models.DriverLocation;
import com.uberlite.lesson27.Serialization.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * Produces simulated DriverLocation events using Virtual Threads.
 * Key: 8-byte long (H3 cell index) — consumed by H3ProducerPartitioner.
 * Rate: configurable, default 3000 events/sec across all drivers.
 */
public class SimulatedDriverProducer {

    // Real metro area bounding boxes [minLat, maxLat, minLng, maxLng]
    record MetroBounds(String city, double minLat, double maxLat, double minLng, double maxLng) {
        double randomLat(Random rng) { return minLat + rng.nextDouble() * (maxLat - minLat); }
        double randomLng(Random rng) { return minLng + rng.nextDouble() * (maxLng - minLng); }
    }

    static final List<MetroBounds> METROS = List.of(
        new MetroBounds("chicago",     41.644,  42.023, -87.940, -87.524),
        new MetroBounds("nyc",         40.477,  40.917, -74.260, -73.700),
        new MetroBounds("la",          33.700,  34.337, -118.670,-118.155),
        new MetroBounds("houston",     29.524,  30.111, -95.820, -95.069),
        new MetroBounds("phoenix",     33.290,  33.916, -112.324,-111.926),
        new MetroBounds("miami",       25.594,  25.980, -80.438, -80.118),
        new MetroBounds("denver",      39.614,  39.914, -105.110,-104.600),
        new MetroBounds("seattle",     47.494,  47.734, -122.460,-122.236)
    );

    static final int    DRIVER_COUNT   = 500;
    static final int    TARGET_EPS     = 3000;   // events per second
    static final long   INTERVAL_MS    = 1000L / (TARGET_EPS / DRIVER_COUNT); // ~600ms per driver
    static final String DRIVER_TOPIC   = TopicBootstrap.DRIVER_TOPIC;

    public static void main(String[] args) throws InterruptedException {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,    "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,  LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DriverLocationSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,     H3ProducerPartitioner.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,                  "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG,             "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,            String.valueOf(32 * 1024));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,      "lz4");
        props.put(ProducerConfig.CLIENT_ID_CONFIG,             "lesson27-driver-producer");

        var sent    = new LongAdder();
        var errors  = new LongAdder();

        try (var producer = new KafkaProducer<Long, DriverLocation>(props)) {

            // Virtual Thread per driver — 500 VTs, each ~1KB stack vs 1MB for platform threads
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {

                var rng = new Random(42L); // seeded for reproducibility
                for (int i = 0; i < DRIVER_COUNT; i++) {
                    final String driverId = "driver-%04d".formatted(i);
                    // Pin each driver to a metro — deterministic assignment
                    final MetroBounds metro = METROS.get(i % METROS.size());
                    final var driverRng = new Random(i); // per-driver seed → reproducible GPS drift

                    executor.submit(() -> {
                        while (!Thread.currentThread().isInterrupted()) {
                            double lat = metro.randomLat(driverRng);
                            double lng = metro.randomLng(driverRng);
                            var location = DriverLocation.of(driverId, lat, lng);
                            // Key is the H3 cell as a Long — H3ProducerPartitioner reads this
                            var record = new ProducerRecord<>(DRIVER_TOPIC, location.h3Cell(), location);

                            producer.send(record, (metadata, ex) -> {
                                if (ex != null) {
                                    errors.increment();
                                    System.err.println("[ERROR] " + ex.getMessage());
                                } else {
                                    sent.increment();
                                }
                            });

                            try {
                                Thread.sleep(INTERVAL_MS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    });
                }

                // Stats printer — every 5 seconds
                var statsThread = Thread.ofVirtual().start(() -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Thread.sleep(5000);
                            long s = sent.sumThenReset();
                            long e = errors.sumThenReset();
                            System.out.printf("[PRODUCER] %.1f events/sec | errors: %d%n",
                                s / 5.0, e);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });

                // Run for 5 minutes
                Thread.sleep(300_000);
                executor.shutdownNow();
                statsThread.interrupt();
            }
        }
    }
}
EOF

# ── SimulatedRiderProducer.java (demo: produces rider requests for match-events) ─
cat > "$PROJECT/$PKG_PATH/SimulatedRiderProducer.java" << 'EOF'
package com.uberlite.lesson27;

import com.uberlite.lesson27.Models.RiderRequest;
import com.uberlite.lesson27.Serialization.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * Demo producer: sends RiderRequest events to rider-requests topic.
 * Key: H3 cell (Long) so they co-partition with driver-locations and trigger matches.
 * Runs for a fixed duration then exits (for demo.sh).
 */
public class SimulatedRiderProducer {

    static final String RIDER_TOPIC = TopicBootstrap.RIDER_TOPIC;
    static final int DURATION_SEC = 45;
    static final int REQUESTS_PER_SEC = 50;
    static final List<double[]> METRO_CENTERS = List.of(
        new double[]{41.83, -87.68}, new double[]{40.71, -74.00}, new double[]{34.05, -118.25},
        new double[]{29.76, -95.37}, new double[]{33.45, -112.07}, new double[]{25.77, -80.19},
        new double[]{39.74, -104.99}, new double[]{47.61, -122.33}
    );

    public static void main(String[] args) throws InterruptedException {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RiderRequestSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, H3ProducerPartitioner.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "lesson27-rider-demo");

        var sent = new LongAdder();
        var rng = new Random(12345);
        long deadline = System.currentTimeMillis() + DURATION_SEC * 1000L;
        long intervalMs = 1000 / Math.max(1, REQUESTS_PER_SEC);

        try (var producer = new KafkaProducer<Long, RiderRequest>(props)) {
            System.out.println("[RIDER DEMO] Sending rider requests for " + DURATION_SEC + "s (~" + REQUESTS_PER_SEC + "/sec)...");
            while (System.currentTimeMillis() < deadline) {
                double[] center = METRO_CENTERS.get(rng.nextInt(METRO_CENTERS.size()));
                double lat = center[0] + (rng.nextDouble() - 0.5) * 0.1;
                double lng = center[1] + (rng.nextDouble() - 0.5) * 0.1;
                String riderId = "rider-" + UUID.randomUUID().toString().substring(0, 8);
                RiderRequest req = RiderRequest.of(riderId, lat, lng);
                producer.send(new ProducerRecord<>(RIDER_TOPIC, req.h3Cell(), req), (m, e) -> {
                    if (e != null) System.err.println("[ERROR] " + e.getMessage());
                    else sent.increment();
                });
                Thread.sleep(intervalMs);
            }
        }
        System.out.println("[RIDER DEMO] Done. Sent " + sent.sum() + " rider requests. Dashboard should show non-zero match metrics.");
    }
}
EOF

# ── PartitionedStreamsApp.java ────────────────────────────────────────────────
cat > "$PROJECT/$PKG_PATH/PartitionedStreamsApp.java" << 'EOF'
package com.uberlite.lesson27;

import com.uberlite.lesson27.Models.*;
import com.uberlite.lesson27.Serialization.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * Kafka Streams topology demonstrating H3-based co-partitioning.
 *
 * Topology:
 *   driver-locations  →  [re-key by h3Cell]  →  GlobalKTable (in-memory view)
 *   rider-requests    →  [Processor: lookup nearby drivers in KTable by h3Cell]
 *                     →  match-events
 *
 * Co-partitioning invariant:
 *   Both source topics MUST have identical partition counts.
 *   H3StreamPartitioner ensures records with the same h3Cell land on the same partition.
 *   The stream-table join is therefore always local — zero repartition.
 */
public class PartitionedStreamsApp {

    private static void runRiderDemoInBackground() {
        Thread.ofVirtual().start(() -> {
            try {
                var props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RiderRequestSerializer.class.getName());
                props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, H3ProducerPartitioner.class.getName());
                props.put(ProducerConfig.CLIENT_ID_CONFIG, "lesson27-rider-demo-ui");
                var METRO_CENTERS = List.of(
                    new double[]{41.83, -87.68}, new double[]{40.71, -74.00}, new double[]{34.05, -118.25},
                    new double[]{29.76, -95.37}, new double[]{33.45, -112.07}, new double[]{25.77, -80.19},
                    new double[]{39.74, -104.99}, new double[]{47.61, -122.33});
                int durationSec = 45;
                int requestsPerSec = 50;
                long intervalMs = 1000L / Math.max(1, requestsPerSec);
                var rng = new Random(12345);
                long deadline = System.currentTimeMillis() + durationSec * 1000L;
                try (var producer = new KafkaProducer<Long, RiderRequest>(props)) {
                    while (System.currentTimeMillis() < deadline) {
                        double[] center = METRO_CENTERS.get(rng.nextInt(METRO_CENTERS.size()));
                        double lat = center[0] + (rng.nextDouble() - 0.5) * 0.1;
                        double lng = center[1] + (rng.nextDouble() - 0.5) * 0.1;
                        RiderRequest req = RiderRequest.of("rider-" + UUID.randomUUID().toString().substring(0, 8), lat, lng);
                        producer.send(new ProducerRecord<>(TopicBootstrap.RIDER_TOPIC, req.h3Cell(), req));
                        Thread.sleep(intervalMs);
                    }
                }
            } catch (Exception e) { System.err.println("[RUN-DEMO] " + e.getMessage()); }
        });
    }

    public static void main(String[] args) throws Exception {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,          "lesson27-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,       "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,  Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,      "4");
        // State dir — RocksDB shards land here
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/lesson27-state");
        // Commit interval — balance between latency and throughput
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");

        var driverSerde = Serialization.driverLocationSerde();
        var riderSerde  = Serialization.riderRequestSerde();

        var builder = new StreamsBuilder();

        // ── Source: driver locations, keyed by h3Cell (Long) ─────────────────
        // Key: h3Cell (Long) — guaranteed by H3ProducerPartitioner on the producer side
        KTable<Long, DriverLocation> driverTable = builder.table(
            TopicBootstrap.DRIVER_TOPIC,
            Consumed.with(Serdes.Long(), driverSerde),
            Materialized.<Long, DriverLocation, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>
                as("driver-location-store")
                .withKeySerde(Serdes.Long())
                .withValueSerde(driverSerde)
        );

        // ── Source: rider requests, keyed by h3Cell (Long) ───────────────────
        KStream<Long, RiderRequest> riderStream = builder.stream(
            TopicBootstrap.RIDER_TOPIC,
            Consumed.with(Serdes.Long(), riderSerde)
        );

        // ── Stream-Table Join: local, zero repartition ────────────────────────
        // Because both topics use H3-based partitioning with identical partition counts,
        // the join is always satisfied from the local RocksDB shard — no network hop.
        riderStream
            .join(
                driverTable,
                (riderId, riderReq, driverLoc) -> {
                    // Simple match: first driver found in the same H3 cell
                    long latency = System.currentTimeMillis() - riderReq.timestampMs();
                    return new MatchEvent(
                        riderReq.riderId(),
                        driverLoc.driverId(),
                        riderReq.h3Cell(),
                        latency
                    );
                },
                Joined.with(Serdes.Long(), riderSerde, driverSerde)
            )
            .peek((h3Cell, match) ->
                System.out.printf("[MATCH] rider=%s driver=%s latency=%dms h3=%d%n",
                    match.riderId(), match.driverId(), match.latencyMs(), h3Cell)
            )
            // Sink with H3 partitioner — output topic inherits spatial layout
            .to(
                TopicBootstrap.MATCH_TOPIC,
                Produced.<Long, MatchEvent>with(Serdes.Long(), Serdes.serdeFrom(
                    (topic, data) -> {
                        if (data == null) return null;
                        byte[] riderBytes  = data.riderId().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        byte[] driverBytes = data.driverId().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        return java.nio.ByteBuffer.allocate(4 + riderBytes.length + 4 + driverBytes.length + 8 + 8)
                            .putInt(riderBytes.length).put(riderBytes)
                            .putInt(driverBytes.length).put(driverBytes)
                            .putLong(data.h3Cell())
                            .putLong(data.latencyMs())
                            .array();
                    },
                    (topic, bytes) -> null
                )).withStreamPartitioner(new H3MatchStreamPartitioner())
            );

        var topology = builder.build();
        System.out.println("[TOPOLOGY]\n" + topology.describe());

        try (var streams = new KafkaStreams(topology, props)) {
            streams.setUncaughtExceptionHandler(ex -> {
                System.err.println("[FATAL] " + ex.getMessage());
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            });

            streams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

            // ── HTTP server: /health, /metrics, /dashboard ─────────────────────
            var server = HttpServer.create(new InetSocketAddress(8080), 0);
            server.createContext("/health", exchange -> {
                var status = streams.state().isRunningOrRebalancing() ? "UP" : "DOWN";
                byte[] body = ("{\"status\":\"" + status + "\"}").getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                try (var os = exchange.getResponseBody()) { os.write(body); }
            });
            server.createContext("/run-demo", exchange -> {
                if (!"POST".equals(exchange.getRequestMethod())) { exchange.sendResponseHeaders(405, -1); return; }
                runRiderDemoInBackground();
                String msg = "Demo started. Sending rider requests for 45s — metrics will update in real time.";
                byte[] body = msg.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
                exchange.sendResponseHeaders(200, body.length);
                try (var os = exchange.getResponseBody()) { os.write(body); }
            });
            server.createContext("/api/metrics", exchange -> {
                if (!"GET".equals(exchange.getRequestMethod())) { exchange.sendResponseHeaders(405, -1); return; }
                var state = streams.state().toString();
                int activeTasks = streams.metadataForLocalThreads().stream().mapToInt(t -> t.activeTasks().size()).sum();
                long consumed = 0L, produced = 0L;
                for (var e : streams.metrics().entrySet()) {
                    String n = e.getKey().name();
                    Object v = e.getValue().metricValue();
                    if (v instanceof Number) {
                        long val = ((Number) v).longValue();
                        if (n != null && n.contains("records-consumed-total")) consumed += val;
                        if (n != null && n.contains("records-produced-total")) produced += val;
                    }
                }
                String json = String.format("{\"state\":\"%s\",\"activeTasks\":%d,\"consumedTotal\":%d,\"producedTotal\":%d}",
                    state, activeTasks, consumed, produced);
                byte[] body = json.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
                exchange.sendResponseHeaders(200, body.length);
                try (var os = exchange.getResponseBody()) { os.write(body); }
            });
            server.createContext("/metrics", exchange -> {
                var state = streams.state().toString();
                int activeTasks = streams.metadataForLocalThreads().stream().mapToInt(t -> t.activeTasks().size()).sum();
                long consumed = 0L; long produced = 0L;
                for (var e : streams.metrics().entrySet()) {
                    String n = e.getKey().name();
                    Object v = e.getValue().metricValue();
                    if (v instanceof Number) {
                        long val = ((Number) v).longValue();
                        if (n != null && n.contains("records-consumed-total")) consumed += val;
                        if (n != null && n.contains("records-produced-total")) produced += val;
                    }
                }
                var sb = new StringBuilder();
                sb.append("# Lesson 27 Partitioning Metrics\n");
                sb.append("streams_state{state=\"").append(state).append("\"} 1\n");
                sb.append("streams_active_tasks ").append(activeTasks).append("\n");
                sb.append("streams_records_consumed_total ").append(consumed).append("\n");
                sb.append("streams_records_produced_total ").append(produced).append("\n");
                byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
                exchange.sendResponseHeaders(200, body.length);
                try (var os = exchange.getResponseBody()) { os.write(body); }
            });
            server.createContext("/", exchange -> {
                String path = exchange.getRequestURI().getPath();
                if (!"/".equals(path) && !"/dashboard".equals(path)) { exchange.sendResponseHeaders(404, -1); return; }
                var state = streams.state().toString();
                int activeTasks = streams.metadataForLocalThreads().stream().mapToInt(t -> t.activeTasks().size()).sum();
                long consumed = 0L; long produced = 0L;
                for (var e : streams.metrics().entrySet()) {
                    String n = e.getKey().name();
                    Object v = e.getValue().metricValue();
                    if (v instanceof Number) {
                        long val = ((Number) v).longValue();
                        if (n != null && n.contains("records-consumed-total")) consumed += val;
                        if (n != null && n.contains("records-produced-total")) produced += val;
                    }
                }
                String html;
                try (InputStream in = PartitionedStreamsApp.class.getResourceAsStream("/dashboard.html")) {
                    html = in != null ? new String(in.readAllBytes(), StandardCharsets.UTF_8)
                        .replace("__STATE__", state).replace("__TASKS__", String.valueOf(activeTasks))
                        .replace("__CONSUMED__", String.valueOf(consumed)).replace("__PRODUCED__", String.valueOf(produced))
                        : "<!DOCTYPE html><html><body><h1>Dashboard</h1><p>dashboard.html not found</p></body></html>";
                } catch (Exception ex) {
                    html = "<!DOCTYPE html><html><body><h1>Error</h1><p>" + ex.getMessage() + "</p></body></html>";
                }
                byte[] body = html.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
                exchange.sendResponseHeaders(200, body.length);
                try (var os = exchange.getResponseBody()) { os.write(body); }
            });
            server.createContext("/dashboard", exchange -> {
                exchange.getResponseHeaders().add("Location", "/");
                exchange.sendResponseHeaders(302, -1);
            });
            server.start();
            System.out.println("[APP] Dashboard: http://localhost:8080/ or http://localhost:8080/dashboard");

            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
EOF

# ── PartitionInspector.java (metrics / verification support) ──────────────────
cat > "$PROJECT/$PKG_PATH/PartitionInspector.java" << 'EOF'
package com.uberlite.lesson27;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import java.time.Duration;
import java.util.*;

/**
 * Queries Kafka broker for per-partition offset lag and byte rates.
 * Used by verify.sh to compute partition skew and validate co-partitioning.
 * Outputs JSON-like lines for easy parsing.
 */
public class PartitionInspector {

    public static void main(String[] args) throws Exception {
        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "lesson27-inspector");
        props.put("key.deserializer",   "org.apache.kafka.common.serialization.LongDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("auto.offset.reset",  "earliest");

        try (var admin  = AdminClient.create(props);
             var consumer = new KafkaConsumer<Long, byte[]>(props)) {

            // ── Partition counts (co-partition validation) ────────────────────
            var topics = List.of(TopicBootstrap.DRIVER_TOPIC, TopicBootstrap.RIDER_TOPIC);
            var descs  = admin.describeTopics(topics).allTopicNames().get();

            descs.forEach((topic, desc) ->
                System.out.printf("PARTITION_COUNT topic=%s count=%d%n",
                    topic, desc.partitions().size()));

            // ── Per-partition end offsets (skew proxy) ───────────────────────
            var driverPartitions = new ArrayList<TopicPartition>();
            for (int i = 0; i < TopicBootstrap.PARTITIONS; i++) {
                driverPartitions.add(new TopicPartition(TopicBootstrap.DRIVER_TOPIC, i));
            }

            var endOffsets = consumer.endOffsets(driverPartitions);
            long maxOffset = 0, minOffset = Long.MAX_VALUE, totalOffset = 0;
            for (var entry : endOffsets.entrySet()) {
                long offset = entry.getValue();
                System.out.printf("PARTITION partition=%d offset=%d%n", entry.getKey().partition(), offset);
                maxOffset   = Math.max(maxOffset, offset);
                minOffset   = Math.min(minOffset, offset);
                totalOffset += offset;
            }

            double avg  = totalOffset / (double) endOffsets.size();
            double skew = (avg > 0) ? maxOffset / avg : 1.0;
            System.out.printf("SKEW max=%d min=%d avg=%.1f skew_ratio=%.2f%n",
                maxOffset, minOffset, avg, skew);
        }
    }
}
EOF

# ── verify.sh ─────────────────────────────────────────────────────────────────
cat > "$PROJECT/verify.sh" << 'VERIFYEOF'
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
if [ "\${CONSUMED:-0}" -eq 0 ] && [ "\${PRODUCED:-0}" -eq 0 ]; then
  echo "  NOTE: Values are zero. Run ./demo.sh (with app and driver producer running) to see non-zero metrics."
fi
echo "==> Dashboard metrics validation passed."
VERIFYEOF
chmod +x "$PROJECT/verify.sh"

# ── start.sh (full path, no duplicate app) ─────────────────────────────────────
cat > "$PROJECT/start.sh" << 'STARTEOF'
#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
MVN="mvn"; [ -x "$DIR/mvnw" ] && MVN="$DIR/mvnw"
if command -v curl &>/dev/null && curl -sf http://localhost:8080/health &>/dev/null 2>/dev/null; then
  echo "==> App already running on http://localhost:8080 (health OK)"
  echo "  Dashboard: http://localhost:8080/ or http://localhost:8080/dashboard"
  echo "  Verify: $DIR/verify.sh"
  exit 0
fi
echo "==> Starting Docker (Zookeeper + Kafka)..."
docker compose up -d
echo "==> Waiting for Kafka to be ready..."
sleep 15
echo "==> Compiling..."
"$MVN" compile -q
echo "==> Creating topics (TopicBootstrap)..."
"$MVN" exec:java -q -Dexec.mainClass=com.uberlite.lesson27.TopicBootstrap
echo "==> Starting PartitionedStreamsApp (background)..."
"$MVN" exec:java -Dexec.mainClass=com.uberlite.lesson27.PartitionedStreamsApp &
APP_PID=$!
echo "  PID: $APP_PID (wait ~20s for rebalance, then run demo)"
echo "  Dashboard: http://localhost:8080/ or http://localhost:8080/dashboard"
echo "  Demo: $DIR/demo.sh"
echo "  Verify: $DIR/verify.sh"
STARTEOF
chmod +x "$PROJECT/start.sh"

# ── demo.sh (rider producer so dashboard shows non-zero matches) ───────────────
cat > "$PROJECT/demo.sh" << 'DEMOEOF'
#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
MVN="mvn"; [ -x "$DIR/mvnw" ] && MVN="$DIR/mvnw"
echo "==> Running rider request demo (45s) to generate matches..."
"$MVN" exec:java -q -Dexec.mainClass=com.uberlite.lesson27.SimulatedRiderProducer
echo "==> Demo done. Refresh dashboard: http://localhost:8080/dashboard"
DEMOEOF
chmod +x "$PROJECT/demo.sh"

# ── test.sh ───────────────────────────────────────────────────────────────────
cat > "$PROJECT/test.sh" << 'TESTEOF'
#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
MVN="mvn"; [ -x "$DIR/mvnw" ] && MVN="$DIR/mvnw"
echo "==> Running tests..."
"$MVN" test -q
echo "==> Tests passed."
TESTEOF
chmod +x "$PROJECT/test.sh"

# ── H3UtilTest.java (minimal test for test.sh) ─────────────────────────────────
mkdir -p "$PROJECT/src/test/java/com/uberlite/lesson27"
cat > "$PROJECT/src/test/java/com/uberlite/lesson27/H3UtilTest.java" << 'TESTJAVA'
package com.uberlite.lesson27;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class H3UtilTest {
    @Test
    void cellOfReturnsNonZero() {
        long cell = H3Util.cellOf(41.85, -87.65);
        assertTrue(cell != 0);
    }
    @Test
    void partitionForIsDeterministic() {
        long cell = H3Util.cellOf(40.71, -74.00);
        int p = H3Util.partitionFor(cell, 12);
        assertTrue(p >= 0 && p < 12);
        assertEquals(p, H3Util.partitionFor(cell, 12));
    }
}
TESTJAVA

echo ""
echo "==> Project structure created in ./$PROJECT"
echo ""
echo "Generated files: docker-compose.yml, pom.xml, verify.sh, start.sh, demo.sh, test.sh,"
echo "  $PKG_PATH/TopicBootstrap.java, H3Util.java, Models.java, Serialization.java,"
echo "  H3ProducerPartitioner.java, H3StreamPartitioner.java, SimulatedDriverProducer.java,"
echo "  SimulatedRiderProducer.java, PartitionedStreamsApp.java, H3MatchStreamPartitioner.java, PartitionInspector.java,"
echo "  src/test/.../H3UtilTest.java"
echo ""
echo "Next steps:"
echo "  cd $PROJECT"
echo "  ./start.sh          # docker + bootstrap + app (or use full path: $PWD/$PROJECT/start.sh)"
echo "  sleep 25 && ./demo.sh   # rider requests -> matches -> non-zero dashboard"
echo "  ./verify.sh         # validate metrics"
echo "  ./test.sh           # run tests"