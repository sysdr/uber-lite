package com.uberlite.lesson29;

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
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public final class MetricsDashboardApp {

    private static final AtomicLong recordsConsumedTotal = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        TopicAdmin.ensureTopics("localhost:9092");

        Thread consumerThread = Thread.ofVirtual().name("dashboard-consumer").start(() -> {
            Thread.currentThread().setContextClassLoader(MetricsDashboardApp.class.getClassLoader());
            var props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "lesson29-dashboard");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            try (var admin = AdminClient.create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
                 var consumer = new KafkaConsumer<byte[], byte[]>(props)) {
                var partitionInfos = admin.describeTopics(List.of(TopicAdmin.DRIVER_LOCATIONS_TOPIC))
                    .topicNameValues().get(TopicAdmin.DRIVER_LOCATIONS_TOPIC).get().partitions();
                Set<TopicPartition> partitions = partitionInfos.stream()
                    .map(p -> new TopicPartition(TopicAdmin.DRIVER_LOCATIONS_TOPIC, p.partition()))
                    .collect(Collectors.toSet());
                consumer.assign(partitions);
                consumer.seekToBeginning(partitions);
                while (!Thread.currentThread().isInterrupted()) {
                    for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(500))) {
                        recordsConsumedTotal.incrementAndGet();
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
            Thread.ofVirtual().start(() -> {
                try { PartitionerDemo.main(new String[0]); } catch (Exception e) { System.err.println("[RUN-DEMO] " + e.getMessage()); }
            });
            byte[] body = "Demo started. Producer sending driver location events. Metrics update every 0.5s.".getBytes(StandardCharsets.UTF_8);
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
            String txt = "# Lesson 29\nstreams_state{state=\"RUNNING\"} 1\nstreams_active_tasks 1\nstreams_records_consumed_total " + c + "\nstreams_records_produced_total " + c + "\n";
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
