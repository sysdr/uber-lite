package com.uberlite;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Serves dashboard on :8080 and consumes driver-updates + rider-requests
 * so metrics reflect co-partitioned traffic in real time.
 */
public final class MetricsDashboardApp {

    private static final AtomicLong driverConsumed = new AtomicLong(0);
    private static final AtomicLong riderConsumed = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        Thread consumerThread = Thread.ofVirtual().name("lesson39-dashboard-consumer").start(() -> {
            Thread.currentThread().setContextClassLoader(MetricsDashboardApp.class.getClassLoader());
            var props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TopicAdmin.BOOTSTRAP);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "lesson39-dashboard");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

            try (var admin = AdminClient.create(
                    Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, TopicAdmin.BOOTSTRAP));
                 var consumer = new KafkaConsumer<byte[], byte[]>(props)) {

                var desc = admin.describeTopics(List.of(TopicAdmin.DRIVER_TOPIC, TopicAdmin.RIDER_TOPIC))
                    .allTopicNames().get(15, TimeUnit.SECONDS);
                var parts = new ArrayList<TopicPartition>();
                for (var topic : List.of(TopicAdmin.DRIVER_TOPIC, TopicAdmin.RIDER_TOPIC)) {
                    int n = desc.get(topic).partitions().size();
                    for (int i = 0; i < n; i++) {
                        parts.add(new TopicPartition(topic, i));
                    }
                }
                consumer.assign(parts);
                consumer.seekToBeginning(parts);

                while (!Thread.currentThread().isInterrupted()) {
                    for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(500))) {
                        if (TopicAdmin.DRIVER_TOPIC.equals(r.topic())) {
                            driverConsumed.incrementAndGet();
                        } else if (TopicAdmin.RIDER_TOPIC.equals(r.topic())) {
                            riderConsumed.incrementAndGet();
                        }
                    }
                }
            } catch (Exception e) {
                if (!Thread.currentThread().isInterrupted()) {
                    System.err.println("[CONSUMER] " + e.getMessage());
                }
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
            Thread.ofVirtual().start(() -> {
                try { DriverProducer.main(new String[0]); } catch (Exception e) {
                    System.err.println("[RUN-DEMO driver] " + e.getMessage());
                }
            });
            Thread.ofVirtual().start(() -> {
                try { RiderProducer.main(new String[0]); } catch (Exception e) {
                    System.err.println("[RUN-DEMO rider] " + e.getMessage());
                }
            });
            byte[] body = "Demo started: DriverProducer + RiderProducer (~30s). Metrics update as records are consumed.".getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });
        server.createContext("/api/metrics", ex -> {
            if (!"GET".equals(ex.getRequestMethod())) { ex.sendResponseHeaders(405, -1); return; }
            long dr = driverConsumed.get();
            long ri = riderConsumed.get();
            long sum = dr + ri;
            String json = ("{\"state\":\"RUNNING\",\"activeTasks\":2,\"driverConsumed\":%d,\"riderConsumed\":%d," +
                          "\"consumedTotal\":%d,\"producedTotal\":%d}").formatted(dr, ri, sum, sum);
            byte[] body = json.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });
        server.createContext("/metrics", ex -> {
            long dr = driverConsumed.get();
            long ri = riderConsumed.get();
            long sum = dr + ri;
            String txt = "# Lesson 39 co-partitioning\n" +
                "lesson39_state{state=\"RUNNING\"} 1\n" +
                "lesson39_active_tasks 2\n" +
                "lesson39_driver_records_consumed_total " + dr + "\n" +
                "lesson39_rider_records_consumed_total " + ri + "\n" +
                "lesson39_records_consumed_total " + sum + "\n";
            byte[] body = txt.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });
        server.createContext("/", ex -> {
            String path = ex.getRequestURI().getPath();
            if (!"/".equals(path) && !"/dashboard".equals(path)) { ex.sendResponseHeaders(404, -1); return; }
            long dr = driverConsumed.get();
            long ri = riderConsumed.get();
            long sum = dr + ri;
            String html;
            try (var in = MetricsDashboardApp.class.getResourceAsStream("/dashboard.html")) {
                html = in != null ? new String(in.readAllBytes(), StandardCharsets.UTF_8)
                    .replace("__STATE__", "RUNNING").replace("__TASKS__", "2")
                    .replace("__DRIVER__", String.valueOf(dr)).replace("__RIDER__", String.valueOf(ri))
                    .replace("__TOTAL__", String.valueOf(sum))
                    : "<html><body><h1>Dashboard</h1><p>dashboard.html not found</p></body></html>";
            } catch (Exception e) { html = "<html><body><h1>Error</h1><p>" + e.getMessage() + "</p></body></html>"; }
            byte[] body = html.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });
        server.createContext("/dashboard", ex -> { ex.getResponseHeaders().add("Location", "/"); ex.sendResponseHeaders(302, -1); });
        server.start();
        System.out.println("[APP] Dashboard: http://localhost:8080/  POST /run-demo or run demo.sh for non-zero metrics.");
        Thread.currentThread().join();
    }
}
