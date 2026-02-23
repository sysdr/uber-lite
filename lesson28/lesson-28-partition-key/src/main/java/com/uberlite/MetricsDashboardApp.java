package com.uberlite;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsDashboardApp {
    private static final String TOPIC = "driver-locations";
    private static final AtomicLong recordsConsumedTotal = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        TopicAdmin.ensureTopic();
        var consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "lesson28-dashboard");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DriverLocationDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        var server = HttpServer.create(new InetSocketAddress(8080), 0);
        server.createContext("/health", ex -> {
            byte[] body = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });
        server.createContext("/run-demo", ex -> {
            if (!"POST".equals(ex.getRequestMethod())) { ex.sendResponseHeaders(405, -1); return; }
            Thread runDemo = new Thread(() -> {
                try { L28ProducerMain.main(new String[0]); } catch (Exception e) { System.err.println("[RUN-DEMO] " + e.getMessage()); }
            }, "run-demo-producer");
            runDemo.setContextClassLoader(Thread.currentThread().getContextClassLoader());
            runDemo.start();
            String msg = "Demo started. Producer sending 10k events. Metrics update every 1s.";
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
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("[APP] Dashboard: http://localhost:8080/  Run demo to see non-zero metrics.");

        // Consumer in background thread; assign all partitions and seek to beginning so we always receive
        Thread consumerThread = new Thread(() -> {
            try (var admin = AdminClient.create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
                 var consumer = new KafkaConsumer<String, DriverLocationEvent>(consumerProps)) {
                var partitionInfos = admin.describeTopics(List.of(TOPIC)).topicNameValues().get(TOPIC).get().partitions();
                Set<TopicPartition> partitions = partitionInfos.stream()
                    .map(p -> new TopicPartition(TOPIC, p.partition()))
                    .collect(Collectors.toSet());
                consumer.assign(partitions);
                consumer.seekToBeginning(partitions);
                while (!Thread.currentThread().isInterrupted()) {
                    for (ConsumerRecord<String, DriverLocationEvent> r : consumer.poll(Duration.ofMillis(500)))
                        recordsConsumedTotal.incrementAndGet();
                }
            } catch (Exception e) {
                System.err.println("[CONSUMER] " + e.getMessage());
            }
        }, "dashboard-consumer");
        consumerThread.setDaemon(false);
        consumerThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(consumerThread::interrupt));
        consumerThread.join();
    }
}
