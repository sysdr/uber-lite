package com.uberlite.lesson27;

import com.uberlite.lesson27.Models.*;
import com.uberlite.lesson27.Serialization.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
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
import java.util.concurrent.atomic.AtomicLong;

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

    /** Counters updated in the topology so dashboard always shows real-time values. */
    private static final AtomicLong recordsConsumedTotal = new AtomicLong(0);
    private static final AtomicLong recordsProducedTotal = new AtomicLong(0);

    private record DashboardMetrics(String state, int activeTasks, long consumedTotal, long producedTotal) {}

    private static DashboardMetrics computeMetrics(KafkaStreams streams) {
        String state = streams.state().toString();
        int activeTasks = streams.metadataForLocalThreads().stream().mapToInt(t -> t.activeTasks().size()).sum();
        return new DashboardMetrics(state, activeTasks, recordsConsumedTotal.get(), recordsProducedTotal.get());
    }

    private static final List<double[]> DEMO_METROS = List.of(
        new double[]{41.83, -87.68}, new double[]{40.71, -74.00}, new double[]{34.05, -118.25},
        new double[]{29.76, -95.37}, new double[]{33.45, -112.07}, new double[]{25.77, -80.19},
        new double[]{39.74, -104.99}, new double[]{47.61, -122.33});

    /** Runs driver + rider producers so KTable has data and matches occur; metrics update in real time. */
    private static void runDemoInBackground() {
        ClassLoader appLoader = PartitionedStreamsApp.class.getClassLoader();
        Thread.ofVirtual().start(() -> {
            Thread.currentThread().setContextClassLoader(appLoader);
            try {
                var p = new Properties();
                p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
                p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DriverLocationSerializer.class.getName());
                p.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, H3ProducerPartitioner.class.getName());
                p.put(ProducerConfig.CLIENT_ID_CONFIG, "lesson27-demo-drivers");
                var rng = new Random(42);
                long end = System.currentTimeMillis() + 60_000;
                try (var producer = new KafkaProducer<Long, DriverLocation>(p)) {
                    for (int i = 0; System.currentTimeMillis() < end; i++) {
                        double[] c = DEMO_METROS.get(i % DEMO_METROS.size());
                        DriverLocation loc = DriverLocation.of("demo-d-" + (i % 80), c[0] + (rng.nextDouble() - 0.5) * 0.1, c[1] + (rng.nextDouble() - 0.5) * 0.1);
                        producer.send(new ProducerRecord<>(TopicBootstrap.DRIVER_TOPIC, loc.h3Cell(), loc));
                        Thread.sleep(12);
                    }
                }
            } catch (Exception e) { System.err.println("[RUN-DEMO drivers] " + e.getMessage()); }
        });
        Thread.ofVirtual().start(() -> {
            Thread.currentThread().setContextClassLoader(appLoader);
            try {
                Thread.sleep(6000);
                var p = new Properties();
                p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
                p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RiderRequestSerializer.class.getName());
                p.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, H3ProducerPartitioner.class.getName());
                p.put(ProducerConfig.CLIENT_ID_CONFIG, "lesson27-demo-riders");
                var rng = new Random(12345);
                long end = System.currentTimeMillis() + 45_000;
                try (var producer = new KafkaProducer<Long, RiderRequest>(p)) {
                    while (System.currentTimeMillis() < end) {
                        double[] c = DEMO_METROS.get(rng.nextInt(DEMO_METROS.size()));
                        RiderRequest req = RiderRequest.of("rider-" + UUID.randomUUID().toString().substring(0, 8), c[0] + (rng.nextDouble() - 0.5) * 0.1, c[1] + (rng.nextDouble() - 0.5) * 0.1);
                        producer.send(new ProducerRecord<>(TopicBootstrap.RIDER_TOPIC, req.h3Cell(), req));
                        Thread.sleep(18);
                    }
                }
            } catch (Exception e) { System.err.println("[RUN-DEMO riders] " + e.getMessage()); }
        });
    }

    private static void ensureTopicsExist() {
        var p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        try (var admin = AdminClient.create(p)) {
            var topics = List.of(
                new NewTopic(TopicBootstrap.DRIVER_TOPIC, TopicBootstrap.PARTITIONS, (short) 1),
                new NewTopic(TopicBootstrap.RIDER_TOPIC, TopicBootstrap.PARTITIONS, (short) 1),
                new NewTopic(TopicBootstrap.MATCH_TOPIC, TopicBootstrap.PARTITIONS, (short) 1));
            try {
                admin.createTopics(topics).all().get();
            } catch (Exception e) {
                if (e.getCause() == null || !e.getCause().getClass().getSimpleName().equals("TopicExistsException")) {
                    System.err.println("[BOOTSTRAP] " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("[BOOTSTRAP] " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        ensureTopicsExist();
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,          "lesson27-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,       "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,  Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,      "4");
        // State dir — RocksDB shards land here
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/lesson27-state");
        // Commit interval — balance between latency and throughput
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500");

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
            .peek((k, v) -> recordsConsumedTotal.incrementAndGet())
            .join(
                driverTable,
                (riderId, riderReq, driverLoc) -> {
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
            .peek((h3Cell, match) -> {
                recordsProducedTotal.incrementAndGet();
                System.out.printf("[MATCH] rider=%s driver=%s latency=%dms h3=%d%n",
                    match.riderId(), match.driverId(), match.latencyMs(), h3Cell);
            })
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
                runDemoInBackground();
                String msg = "Demo started. Drivers (60s) + riders (45s). Metrics update every 0.5s.";
                byte[] body = msg.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
                exchange.sendResponseHeaders(200, body.length);
                try (var os = exchange.getResponseBody()) { os.write(body); }
            });
            server.createContext("/api/metrics", exchange -> {
                if (!"GET".equals(exchange.getRequestMethod())) { exchange.sendResponseHeaders(405, -1); return; }
                var m = computeMetrics(streams);
                String json = String.format("{\"state\":\"%s\",\"activeTasks\":%d,\"consumedTotal\":%d,\"producedTotal\":%d}",
                    m.state(), m.activeTasks(), m.consumedTotal(), m.producedTotal());
                byte[] body = json.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
                exchange.sendResponseHeaders(200, body.length);
                try (var os = exchange.getResponseBody()) { os.write(body); }
            });
            server.createContext("/metrics", exchange -> {
                var m = computeMetrics(streams);
                var sb = new StringBuilder();
                sb.append("# Lesson 27 Partitioning Metrics\n");
                sb.append("streams_state{state=\"").append(m.state()).append("\"} 1\n");
                sb.append("streams_active_tasks ").append(m.activeTasks()).append("\n");
                sb.append("streams_records_consumed_total ").append(m.consumedTotal()).append("\n");
                sb.append("streams_records_produced_total ").append(m.producedTotal()).append("\n");
                byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
                exchange.sendResponseHeaders(200, body.length);
                try (var os = exchange.getResponseBody()) { os.write(body); }
            });
            server.createContext("/", exchange -> {
                String path = exchange.getRequestURI().getPath();
                if (!"/".equals(path) && !"/dashboard".equals(path)) { exchange.sendResponseHeaders(404, -1); return; }
                var m = computeMetrics(streams);
                String html;
                try (InputStream in = PartitionedStreamsApp.class.getResourceAsStream("/dashboard.html")) {
                    html = in != null ? new String(in.readAllBytes(), StandardCharsets.UTF_8)
                        .replace("__STATE__", m.state()).replace("__TASKS__", String.valueOf(m.activeTasks()))
                        .replace("__CONSUMED__", String.valueOf(m.consumedTotal())).replace("__PRODUCED__", String.valueOf(m.producedTotal()))
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
