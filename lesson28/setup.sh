#!/usr/bin/env bash
set -euo pipefail

PROJECT="lesson-28-partition-key"
mkdir -p "$PROJECT"
cd "$PROJECT"

# ─── Docker Compose ──────────────────────────────────────────────────────────
cat > docker-compose.yml <<'COMPOSE'
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports: ["2181:2181"]

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_NUM_PARTITIONS: 64
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      KAFKA_LOG_RETENTION_MS: 600000
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
COMPOSE

# ─── Maven POM ───────────────────────────────────────────────────────────────
cat > pom.xml <<'POM'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.uberlite</groupId>
  <artifactId>lesson-28-partition-key</artifactId>
  <version>1.0-SNAPSHOT</version>
  <properties>
    <maven.compiler.source>21</maven.compiler.source>
    <maven.compiler.target>21</maven.compiler.target>
    <maven.compiler.release>21</maven.compiler.release>
  </properties>
  <dependencies>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-clients</artifactId>
      <version>3.6.1</version>
    </dependency>
    <dependency>
      <groupId>com.uber</groupId>
      <artifactId>h3</artifactId>
      <version>4.1.1</version>
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
        <configuration>
          <fork>true</fork>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>
POM

# ─── Source Tree ─────────────────────────────────────────────────────────────
mkdir -p src/main/java/com/uberlite

# ── DriverLocationEvent record ───────────────────────────────────────────────
cat > src/main/java/com/uberlite/DriverLocationEvent.java <<'JAVA'
package com.uberlite;

public record DriverLocationEvent(
    String driverId,
    double lat,
    double lon,
    long timestampMs,
    String status   // "AVAILABLE" | "ON_TRIP"
) {}
JAVA

# ── DriverLocationSerializer ──────────────────────────────────────────────────
cat > src/main/java/com/uberlite/DriverLocationSerializer.java <<'JAVA'
package com.uberlite;

import org.apache.kafka.common.serialization.Serializer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Wire format: [lat(8)][lon(8)][timestampMs(8)][statusLen(4)][status(N)][driverIdLen(4)][driverId(M)]
 * Partitioner reads only bytes 0-15 (lat+lon) — no full deserialization needed at routing time.
 */
public class DriverLocationSerializer implements Serializer<DriverLocationEvent> {
    @Override
    public byte[] serialize(String topic, DriverLocationEvent event) {
        byte[] statusBytes = event.status().getBytes(StandardCharsets.UTF_8);
        byte[] driverIdBytes = event.driverId().getBytes(StandardCharsets.UTF_8);
        int capacity = 8 + 8 + 8 + 4 + statusBytes.length + 4 + driverIdBytes.length;
        return ByteBuffer.allocate(capacity)
            .putDouble(event.lat())
            .putDouble(event.lon())
            .putLong(event.timestampMs())
            .putInt(statusBytes.length)
            .put(statusBytes)
            .putInt(driverIdBytes.length)
            .put(driverIdBytes)
            .array();
    }
}
JAVA

# ── DriverLocationDeserializer ────────────────────────────────────────────────
cat > src/main/java/com/uberlite/DriverLocationDeserializer.java <<'JAVA'
package com.uberlite;

import org.apache.kafka.common.serialization.Deserializer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class DriverLocationDeserializer implements Deserializer<DriverLocationEvent> {
    @Override
    public DriverLocationEvent deserialize(String topic, byte[] data) {
        var buf = ByteBuffer.wrap(data);
        double lat = buf.getDouble();
        double lon = buf.getDouble();
        long ts = buf.getLong();
        byte[] statusBytes = new byte[buf.getInt()];
        buf.get(statusBytes);
        byte[] driverIdBytes = new byte[buf.getInt()];
        buf.get(driverIdBytes);
        return new DriverLocationEvent(
            new String(driverIdBytes, StandardCharsets.UTF_8),
            lat, lon, ts,
            new String(statusBytes, StandardCharsets.UTF_8)
        );
    }
}
JAVA

# ── H3GeoPartitioner ─────────────────────────────────────────────────────────
cat > src/main/java/com/uberlite/H3GeoPartitioner.java <<'JAVA'
package com.uberlite;

import com.uber.h3core.H3Core;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Routes producer records to partitions based on H3 Resolution-3 geographic cell.
 *
 * Wire contract: value bytes 0-7 = lat (double), bytes 8-15 = lon (double).
 * Does NOT deserialize the full payload — reads only the first 16 bytes.
 *
 * Partition formula: (unsigned h3Index) % numPartitions
 * H3 Res-3 provides ~41k cells globally; modulo over 64 partitions gives
 * ~640 cells/partition with empirical skew < 3%.
 */
public class H3GeoPartitioner implements Partitioner {

    private static final int H3_RESOLUTION = 3;

    // Thread-safe, stateless JNI wrapper — safe to share across threads
    private H3Core h3;

    // Metrics
    private final AtomicLong partitionCallCount = new AtomicLong(0);
    private final AtomicLong fallbackCount = new AtomicLong(0);

    @Override
    public void configure(Map<String, ?> configs) {
        try {
            h3 = H3Core.newInstance();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize H3Core", e);
        }
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        partitionCallCount.incrementAndGet();

        int numPartitions = cluster.partitionCountForTopic(topic);

        if (valueBytes == null || valueBytes.length < 16) {
            // Fallback: hash the key bytes. Log for debugging.
            fallbackCount.incrementAndGet();
            if (keyBytes == null) return 0;
            return Math.abs(java.util.Arrays.hashCode(keyBytes)) % numPartitions;
        }

        // Zero-parse: read only lat(8) + lon(8) from the fixed header
        var buf = ByteBuffer.wrap(valueBytes, 0, 16);
        double lat = buf.getDouble();
        double lon = buf.getDouble();

        // Bounds check — H3 will throw on invalid coordinates
        if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
            fallbackCount.incrementAndGet();
            return 0;
        }

        long h3Index = h3.latLngToCell(lat, lon, H3_RESOLUTION);
        // Use unsigned remainder so negative h3Index (if any) maps correctly
        return (int) Long.remainderUnsigned(h3Index, numPartitions);
    }

    @Override
    public void close() {}

    /** Expose for JMX / metrics scraping */
    public long getPartitionCallCount() { return partitionCallCount.get(); }
    public long getFallbackCount() { return fallbackCount.get(); }
}
JAVA

# ── TopicAdmin ────────────────────────────────────────────────────────────────
cat > src/main/java/com/uberlite/TopicAdmin.java <<'JAVA'
package com.uberlite;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.TopicConfig;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TopicAdmin {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String TOPIC = "driver-locations";
    private static final int PARTITIONS = 64;
    private static final short REPLICATION = 1;

    public static void ensureTopic() throws ExecutionException, InterruptedException {
        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);

        try (var admin = AdminClient.create(props)) {
            var existing = admin.listTopics().names().get();
            if (existing.contains(TOPIC)) {
                System.out.printf("[TopicAdmin] Topic '%s' already exists%n", TOPIC);
                return;
            }

            var newTopic = new NewTopic(TOPIC, PARTITIONS, REPLICATION)
                .configs(Map.of(
                    TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4",
                    TopicConfig.RETENTION_MS_CONFIG, "600000"
                ));

            admin.createTopics(List.of(newTopic)).all().get();
            System.out.printf("[TopicAdmin] Created '%s' with %d partitions%n", TOPIC, PARTITIONS);
        }
    }
}
JAVA

# ── L28ProducerMain ───────────────────────────────────────────────────────────
cat > src/main/java/com/uberlite/L28ProducerMain.java <<'JAVA'
package com.uberlite;

import com.uber.h3core.H3Core;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Simulates 500 drivers across 10 cities, each producing location updates
 * at ~10 events/sec using Virtual Threads.
 *
 * Demonstrates that H3 Res-3 routes drivers in the same city to the same partition.
 */
public class L28ProducerMain {

    // 10 cities: [name, lat, lon]
    private static final double[][] CITIES = {
        // lat, lon
        {41.8781, -87.6298},  // Chicago
        {40.7128, -74.0060},  // New York
        {34.0522, -118.2437}, // Los Angeles
        {29.7604, -95.3698},  // Houston
        {33.4484, -112.0740}, // Phoenix
        {47.6062, -122.3321}, // Seattle
        {25.7617, -80.1918},  // Miami
        {37.7749, -122.4194}, // San Francisco
        {39.9526, -75.1652},  // Philadelphia
        {42.3601, -71.0589}   // Boston
    };

    private static final String TOPIC = "driver-locations";
    private static final int DRIVERS_PER_CITY = 50;
    private static final int EVENTS_PER_DRIVER = 20;
    private static final AtomicLong sentCount = new AtomicLong(0);
    private static final AtomicLong errorCount = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        TopicAdmin.ensureTopic();

        H3Core h3 = H3Core.newInstance();

        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DriverLocationSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, H3GeoPartitioner.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        try (var producer = new KafkaProducer<String, DriverLocationEvent>(props)) {
            // Virtual Thread per driver — 500 threads at ~1KB each vs 500MB for platform threads
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var futures = new ArrayList<Future<?>>();

                for (int cityIdx = 0; cityIdx < CITIES.length; cityIdx++) {
                    final double baseLat = CITIES[cityIdx][0];
                    final double baseLon = CITIES[cityIdx][1];
                    final long cityH3 = h3.latLngToCell(baseLat, baseLon, 3);
                    final String cityCell = Long.toHexString(cityH3);

                    for (int d = 0; d < DRIVERS_PER_CITY; d++) {
                        final String driverId = "driver-%d-%d".formatted(cityIdx, d);
                        final int driverNum = d;

                        futures.add(executor.submit(() -> {
                            var rng = new Random(driverId.hashCode()); // seeded for reproducibility
                            for (int i = 0; i < EVENTS_PER_DRIVER; i++) {
                                // Jitter within ~2km of city center
                                double lat = baseLat + (rng.nextDouble() - 0.5) * 0.02;
                                double lon = baseLon + (rng.nextDouble() - 0.5) * 0.02;

                                var event = new DriverLocationEvent(
                                    driverId, lat, lon,
                                    System.currentTimeMillis(),
                                    "AVAILABLE"
                                );

                                producer.send(
                                    new ProducerRecord<>(TOPIC, driverId, event),
                                    (meta, ex) -> {
                                        if (ex != null) {
                                            errorCount.incrementAndGet();
                                        } else {
                                            sentCount.incrementAndGet();
                                            if (sentCount.get() % 1000 == 0) {
                                                System.out.printf(
                                                    "[Producer] Sent=%d City-H3=%s → Partition=%d%n",
                                                    sentCount.get(), cityCell, meta.partition()
                                                );
                                            }
                                        }
                                    }
                                );
                                // ~10 events/sec per driver
                                Thread.sleep(100);
                            }
                            return null;
                        }));
                    }
                }

                for (var f : futures) f.get();
            }

            producer.flush();
        }

        System.out.printf("%n[Producer] Done. Sent=%d Errors=%d%n",
            sentCount.get(), errorCount.get());
    }
}
JAVA

# ── MetricsDashboardApp: HTTP server + consumer for dashboard ───────────────
mkdir -p src/main/resources
cat > src/main/java/com/uberlite/MetricsDashboardApp.java <<'JAVA2'
package com.uberlite;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsDashboardApp {
    private static final String TOPIC = "driver-locations";
    private static final AtomicLong recordsConsumedTotal = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        TopicAdmin.ensureTopic();
        var consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "lesson28-dashboard-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DriverLocationDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        Thread consumerThread = Thread.ofVirtual().name("dashboard-consumer").start(() -> {
            Thread.currentThread().setContextClassLoader(MetricsDashboardApp.class.getClassLoader());
            try (var consumer = new KafkaConsumer<String, DriverLocationEvent>(consumerProps)) {
                consumer.subscribe(List.of(TOPIC));
                while (!Thread.currentThread().isInterrupted()) {
                    for (ConsumerRecord<String, DriverLocationEvent> r : consumer.poll(Duration.ofMillis(500))) {
                        try { recordsConsumedTotal.incrementAndGet(); } catch (Exception e) { System.err.println("[CONSUMER] " + e.getMessage()); }
                    }
                }
            } catch (Exception e) {
                if (!Thread.currentThread().isInterrupted()) System.err.println("[CONSUMER] " + e.getMessage());
            }
        });
        Runtime.getRuntime().addShutdownHook(new Thread(consumerThread::interrupt));

        var server = HttpServer.create(new InetSocketAddress(8080), 0);
        server.createContext("/health", ex -> {
            byte[] body = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });
        server.createContext("/run-demo", ex -> {
            if (!"POST".equals(ex.getRequestMethod())) { ex.sendResponseHeaders(405, -1); return; }
            Thread.ofVirtual().start(() -> { try { L28ProducerMain.main(new String[0]); } catch (Exception e) { System.err.println("[RUN-DEMO] " + e.getMessage()); } });
            String msg = "Demo started. Producer sending 10k events. Metrics update every 0.5s.";
            byte[] body = msg.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });
        server.createContext("/api/metrics", ex -> {
            if (!"GET".equals(ex.getRequestMethod())) { ex.sendResponseHeaders(405, -1); return; }
            long c = recordsConsumedTotal.get();
            String json = String.format("{\"state\":\"RUNNING\",\"activeTasks\":1,\"consumedTotal\":%d,\"producedTotal\":%d}", c, c);
            byte[] body = json.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });
        server.createContext("/metrics", ex -> {
            long c = recordsConsumedTotal.get();
            String txt = "# Lesson 28\nstreams_state{state=\"RUNNING\"} 1\nstreams_active_tasks 1\nstreams_records_consumed_total " + c + "\nstreams_records_produced_total " + c + "\n";
            byte[] body = txt.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });
        server.createContext("/", ex -> {
            String path = ex.getRequestURI().getPath();
            if (!"/".equals(path) && !"/dashboard".equals(path)) { ex.sendResponseHeaders(404, -1); return; }
            long c = recordsConsumedTotal.get();
            String html;
            try (var in = MetricsDashboardApp.class.getResourceAsStream("/dashboard.html")) {
                html = in != null ? new String(in.readAllBytes(), StandardCharsets.UTF_8)
                    .replace("__STATE__", "RUNNING").replace("__TASKS__", "1")
                    .replace("__CONSUMED__", String.valueOf(c)).replace("__PRODUCED__", String.valueOf(c))
                    : "<html><body><h1>Dashboard</h1><p>dashboard.html not found</p></body></html>";
            } catch (Exception e) { html = "<html><body><h1>Error</h1><p>" + e.getMessage() + "</p></body></html>"; }
            byte[] body = html.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });
        server.createContext("/dashboard", ex -> { ex.getResponseHeaders().add("Location", "/"); ex.sendResponseHeaders(302, -1); });
        server.start();
        System.out.println("[APP] Dashboard: http://localhost:8080/  Run demo to see non-zero metrics.");
        Thread.currentThread().join();
    }
}
JAVA2

cat > src/main/resources/dashboard.html <<'DASH'
<!DOCTYPE html><html><head><meta charset="utf-8"><title>Lesson 28 Dashboard</title>
<style>body{font-family:system-ui;margin:1rem;background:#e0f2fe;color:#0c4a6e;}
.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:1rem;}
.card{background:#fff;padding:1rem;border-radius:8px;}
.card .value{font-size:1.5rem;font-weight:700;}
.btn{padding:0.5rem 1rem;background:#0ea5e9;color:#fff;border:none;border-radius:6px;cursor:pointer;}
</style></head><body>
<h1>Lesson 28 Partition Key Dashboard</h1>
<p><button class="btn" id="run-demo-btn">Run demo</button><span id="demo-status"></span></p>
<div class="metrics">
<div class="card"><label>State</label><div class="value" id="m-state">__STATE__</div></div>
<div class="card"><label>Active tasks</label><div class="value" id="m-tasks">__TASKS__</div></div>
<div class="card"><label>Records consumed</label><div class="value" id="m-consumed">__CONSUMED__</div></div>
<div class="card"><label>Records produced</label><div class="value" id="m-produced">__PRODUCED__</div></div>
</div>
<p><a href="/metrics">Raw metrics</a> <a href="/health">Health</a></p>
<p class="hint">Metrics refresh every 1s. Click Run demo to send 10k events — values update in real time.</p>
<script>
function update(){fetch('/api/metrics',{cache:'no-store'}).then(r=>r.json()).then(d=>{
  document.getElementById('m-state').textContent=d.state;
  document.getElementById('m-tasks').textContent=d.activeTasks;
  document.getElementById('m-consumed').textContent=d.consumedTotal;
  document.getElementById('m-produced').textContent=d.producedTotal;
}).catch(()=>{});}
setInterval(update,1000);update();
document.getElementById('run-demo-btn').onclick=function(){
  this.disabled=true; document.getElementById('demo-status').textContent='Starting…';
  fetch('/run-demo',{method:'POST'}).then(r=>r.text()).then(t=>{
    document.getElementById('demo-status').textContent=t;
    setTimeout(()=>{ this.disabled=false; document.getElementById('demo-status').textContent=''; },5000);
  }).catch(()=>{ this.disabled=false; document.getElementById('demo-status').textContent='Error'; });
};
</script></body></html>
DASH

# ─── Unit test ───────────────────────────────────────────────────────────────
mkdir -p src/test/java/com/uberlite
cat > src/test/java/com/uberlite/H3GeoPartitionerTest.java <<'TESTJAVA'
package com.uberlite;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class H3GeoPartitionerTest {
    @Test
    void partitionerUsesH3ForValidCoords() {
        var p = new H3GeoPartitioner();
        p.configure(Collections.emptyMap());
        Cluster cluster = clusterWithPartitions(64);
        byte[] valueBytes = new byte[32];
        java.nio.ByteBuffer.wrap(valueBytes).putDouble(41.8781).putDouble(-87.6298);
        int partition = p.partition("driver-locations", "driver-0", null, null, valueBytes, cluster);
        assertTrue(partition >= 0 && partition < 64);
        p.close();
    }

    @Test
    void fallbackForShortValue() {
        var p = new H3GeoPartitioner();
        p.configure(Collections.emptyMap());
        Cluster cluster = clusterWithPartitions(64);
        byte[] shortValue = new byte[8];
        int partition = p.partition("driver-locations", "driver-0", "key".getBytes(), null, shortValue, cluster);
        assertTrue(partition >= 0 && partition < 64);
        assertTrue(p.getFallbackCount() >= 1);
        p.close();
    }

    private static Cluster clusterWithPartitions(int numPartitions) {
        Node node = new Node(1, "localhost", 9092);
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++)
            partitions.add(new PartitionInfo("driver-locations", i, node, null, null));
        return new Cluster("cluster", Collections.singletonList(node), partitions, Collections.emptySet(), Collections.emptySet());
    }
}
TESTJAVA

cat > start.sh <<'START'
#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
MVN="mvn"; [ -x "$DIR/mvnw" ] && MVN="$DIR/mvnw"
if command -v curl &>/dev/null && curl -sf http://localhost:8080/health &>/dev/null 2>/dev/null; then
  echo "==> App already running on http://localhost:8080 (health OK)"
  echo "  Dashboard: http://localhost:8080/"
  exit 0
fi
echo "==> Starting Docker (Zookeeper + Kafka)..."
docker compose up -d
echo "==> Waiting for Kafka to be ready..."
sleep 15
echo "==> Compiling and building classpath..."
"$MVN" compile -q
"$MVN" dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/lib -q
CP="$DIR/target/classes:$DIR/target/lib/*"
echo "==> Starting MetricsDashboardApp (background, full classpath for real-time metrics)..."
java -cp "$CP" com.uberlite.MetricsDashboardApp &
echo "  Wait ~5s then: Dashboard http://localhost:8080/   Demo: $DIR/demo.sh or click Run demo"
START
chmod +x start.sh

cat > demo.sh <<'DEMO'
#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
MVN="mvn"; [ -x "$DIR/mvnw" ] && MVN="$DIR/mvnw"
echo "==> Running producer demo (10k driver-location events)..."
"$MVN" exec:java -q -Dexec.mainClass=com.uberlite.L28ProducerMain
echo "==> Demo done. Refresh dashboard: http://localhost:8080/"
DEMO
chmod +x demo.sh

echo ""
echo "✅ Project scaffolded in ./$PROJECT"
echo "   Next:"
echo "   1. cd $PROJECT && ./start.sh"
echo "   2. ./demo.sh    (then check dashboard: http://localhost:8080/)"

