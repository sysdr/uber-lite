package com.uberlite.batching;

import com.sun.net.httpserver.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringSerializer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.*;

    /**
     * Simple HTTP server exposing Kafka producer metrics on port 8081.
     * Access via: curl http://localhost:8081/metrics
     */
public class MetricsServer {
    private final KafkaProducer<String, DriverLocationUpdate> producer;
    private final HttpServer server;
    
    public MetricsServer(Properties config) throws IOException {
        this.producer = new KafkaProducer<>(config);
        this.server = HttpServer.create(new InetSocketAddress(8081), 0);
        this.server.createContext("/metrics", this::handleMetrics);
        this.server.setExecutor(null);
    }
    
    private void handleMetrics(HttpExchange exchange) throws IOException {
        var metrics = producer.metrics();
        var response = formatMetrics(metrics);
        
        exchange.getResponseHeaders().add("Content-Type", "text/plain");
        exchange.sendResponseHeaders(200, response.length());
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }
    }
    
    private String formatMetrics(Map<MetricName, ? extends Metric> metrics) {
        var sb = new StringBuilder();
        sb.append("=".repeat(80)).append("\n");
        sb.append("KAFKA PRODUCER BATCHING METRICS\n");
        sb.append("=".repeat(80)).append("\n\n");
        
        // Key batching metrics
        appendMetric(sb, metrics, "batch-size-avg", "Average Batch Size (bytes)");
        appendMetric(sb, metrics, "batch-size-max", "Max Batch Size (bytes)");
        appendMetric(sb, metrics, "records-per-request-avg", "Avg Records per Request");
        appendMetric(sb, metrics, "record-queue-time-avg", "Avg Queue Time (ms)");
        appendMetric(sb, metrics, "record-queue-time-max", "Max Queue Time (ms)");
        appendMetric(sb, metrics, "buffer-available-bytes", "Available Buffer (bytes)");
        appendMetric(sb, metrics, "bufferpool-wait-time-total", "Buffer Wait Time (ms)");
        
        sb.append("\n").append("-".repeat(80)).append("\n");
        sb.append("THROUGHPUT METRICS\n");
        sb.append("-".repeat(80)).append("\n\n");
        
        appendMetric(sb, metrics, "record-send-rate", "Record Send Rate (rec/sec)");
        appendMetric(sb, metrics, "record-send-total", "Total Records Sent");
        appendMetric(sb, metrics, "request-rate", "Request Rate (req/sec)");
        appendMetric(sb, metrics, "request-total", "Total Requests");
        
        return sb.toString();
    }
    
    private void appendMetric(StringBuilder sb, Map<MetricName, ? extends Metric> metrics, 
                             String metricName, String displayName) {
        var metric = findMetric(metrics, metricName);
        if (metric != null) {
            sb.append(String.format("%-40s: ", displayName));
            Object value = metric.metricValue();
            if (value instanceof Double) {
                sb.append(String.format("%.2f", value));
            } else {
                sb.append(value);
            }
            sb.append("\n");
        }
    }
    
    private Metric findMetric(Map<MetricName, ? extends Metric> metrics, String name) {
        return metrics.entrySet().stream()
            .filter(e -> e.getKey().name().equals(name))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(null);
    }
    
    public void start() {
        server.start();
        System.out.println("Metrics server running on http://localhost:8081/metrics");
    }
    
    public void stop() {
        server.stop(0);
        producer.close();
    }
    
    public static void main(String[] args) throws IOException {
        var config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);
        
        var server = new MetricsServer(config);
        server.start();
        
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        
        // Keep alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            server.stop();
        }
    }
}
