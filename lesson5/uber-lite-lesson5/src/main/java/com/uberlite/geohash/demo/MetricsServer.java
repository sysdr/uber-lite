package com.uberlite.geohash.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.sun.net.httpserver.HttpServer;
import com.uberlite.geohash.model.PartitionMetric;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsServer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String METRICS_TOPIC = "partition-metrics";
    private static final ObjectMapper mapper = new ObjectMapper();
    
    private static volatile MetricsData cachedMetrics = new MetricsData();
    private static volatile String lastError;
    
    public static void main(String[] args) throws IOException {
        var server = HttpServer.create(new InetSocketAddress("0.0.0.0", 8080), 0);
        
        // Start metrics consumer
        new Thread(() -> startMetricsConsumer()).start();
        
        // JSON API endpoint
        server.createContext("/api/metrics", exchange -> {
            try {
                var json = formatJsonMetrics(cachedMetrics);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                exchange.getResponseHeaders().add("Cache-Control", "no-cache");
                byte[] bytes = json.getBytes();
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            } catch (Exception e) {
                e.printStackTrace();
                ObjectNode error = mapper.createObjectNode();
                error.put("error", e.getMessage());
                error.put("timestamp", System.currentTimeMillis());
                byte[] bytes = error.toString().getBytes();
                exchange.sendResponseHeaders(503, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
        });
        
        // Dashboard HTML
        server.createContext("/dashboard", exchange -> {
            var html = getDashboardHtml();
            exchange.getResponseHeaders().add("Content-Type", "text/html");
            byte[] bytes = html.getBytes();
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
        
        server.createContext("/", exchange -> {
            exchange.getResponseHeaders().add("Location", "/dashboard");
            exchange.sendResponseHeaders(302, 0);
            exchange.close();
        });
        
        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        server.start();
        
        System.out.println("Metrics server started on port 8080");
        System.out.println("  Dashboard: http://localhost:8080/dashboard");
        System.out.println("  Metrics API: http://localhost:8080/api/metrics");
    }
    
    private static void startMetricsConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "metrics-dashboard-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(METRICS_TOPIC));
            
            Map<String, PartitionMetric> geohashMetrics = new ConcurrentHashMap<>();
            Map<String, PartitionMetric> h3Metrics = new ConcurrentHashMap<>();
            
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));
                for (var record : records) {
                    try {
                        PartitionMetric metric = mapper.readValue(record.value(), PartitionMetric.class);
                        String key = metric.partitionerType() + "_" + metric.partition() + "_" + metric.latitudeBand();
                        
                        if ("geohash".equals(metric.partitionerType())) {
                            geohashMetrics.put(key, metric);
                        } else if ("h3".equals(metric.partitionerType())) {
                            h3Metrics.put(key, metric);
                        }
                        
                        cachedMetrics = new MetricsData(geohashMetrics, h3Metrics);
                        lastError = null;
                    } catch (Exception e) {
                        lastError = e.getMessage();
                    }
                }
            }
        } catch (Exception e) {
            lastError = e.getMessage();
            e.printStackTrace();
        }
    }
    
    private static String formatJsonMetrics(MetricsData metrics) throws Exception {
        ObjectNode root = mapper.createObjectNode();
        root.put("timestamp", System.currentTimeMillis());
        
        // Geohash metrics
        ArrayNode geohashArray = root.putArray("geohash");
        long geohashTotal = 0;
        Map<Integer, Long> geohashByPartition = new HashMap<>();
        for (PartitionMetric m : metrics.geohashMetrics.values()) {
            geohashTotal += m.recordCount();
            geohashByPartition.put(m.partition(), 
                geohashByPartition.getOrDefault(m.partition(), 0L) + m.recordCount());
            
            ObjectNode node = geohashArray.addObject();
            node.put("partitioner", m.partitionerType());
            node.put("partition", m.partition());
            node.put("latitudeBand", m.latitudeBand());
            node.put("recordCount", m.recordCount());
            node.put("timestamp", m.timestamp());
        }
        
        // H3 metrics
        ArrayNode h3Array = root.putArray("h3");
        long h3Total = 0;
        Map<Integer, Long> h3ByPartition = new HashMap<>();
        for (PartitionMetric m : metrics.h3Metrics.values()) {
            h3Total += m.recordCount();
            h3ByPartition.put(m.partition(), 
                h3ByPartition.getOrDefault(m.partition(), 0L) + m.recordCount());
            
            ObjectNode node = h3Array.addObject();
            node.put("partitioner", m.partitionerType());
            node.put("partition", m.partition());
            node.put("latitudeBand", m.latitudeBand());
            node.put("recordCount", m.recordCount());
            node.put("timestamp", m.timestamp());
        }
        
        // Summary statistics
        ObjectNode summary = root.putObject("summary");
        summary.put("geohashTotalRecords", geohashTotal);
        summary.put("h3TotalRecords", h3Total);
        summary.put("geohashPartitions", geohashByPartition.size());
        summary.put("h3Partitions", h3ByPartition.size());
        
        // Calculate partition skew (standard deviation)
        if (!geohashByPartition.isEmpty()) {
            double geohashMean = geohashTotal / (double) geohashByPartition.size();
            double geohashVariance = geohashByPartition.values().stream()
                .mapToDouble(v -> Math.pow(v - geohashMean, 2))
                .average().orElse(0);
            summary.put("geohashSkew", Math.sqrt(geohashVariance));
        }
        
        if (!h3ByPartition.isEmpty()) {
            double h3Mean = h3Total / (double) h3ByPartition.size();
            double h3Variance = h3ByPartition.values().stream()
                .mapToDouble(v -> Math.pow(v - h3Mean, 2))
                .average().orElse(0);
            summary.put("h3Skew", Math.sqrt(h3Variance));
        }
        
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
    }
    
    private static String getDashboardHtml() {
        return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Geohash Limits Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        html {
            background-color: #F2E9F7;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background-color: #F2E9F7;
            padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { color: #2c3e50; text-align: center; margin-bottom: 30px; }
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .metric-card {
            background: white;
            border-radius: 12px;
            padding: 25px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .metric-label {
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
            margin-bottom: 10px;
        }
        .metric-value {
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
        }
        .comparison-table {
            background: white;
            border-radius: 12px;
            padding: 25px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f8f9fa; font-weight: 600; }
        .status { text-align: center; color: white; padding: 10px; margin-bottom: 20px; border-radius: 8px; }
        .status.connected { background: #28a745; }
        .status.error { background: #dc3545; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🗺️ Geohash Limits Dashboard</h1>
        <div id="status" class="status">Loading...</div>
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-label">Geohash Total Records</div>
                <div class="metric-value" id="geohashTotal">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">H3 Total Records</div>
                <div class="metric-value" id="h3Total">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Geohash Skew</div>
                <div class="metric-value" id="geohashSkew">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">H3 Skew</div>
                <div class="metric-value" id="h3Skew">0</div>
            </div>
        </div>
        <div class="comparison-table">
            <h2>Partition Metrics</h2>
            <table>
                <thead>
                    <tr>
                        <th>Partitioner</th>
                        <th>Partition</th>
                        <th>Latitude Band</th>
                        <th>Record Count</th>
                    </tr>
                </thead>
                <tbody id="metricsBody">
                    <tr><td colspan="4" style="text-align: center;">Loading...</td></tr>
                </tbody>
            </table>
        </div>
    </div>
    <script>
        function formatNumber(num) {
            return new Intl.NumberFormat().format(num);
        }
        
        async function fetchMetrics() {
            try {
                const response = await fetch('/api/metrics?t=' + Date.now());
                const data = await response.json();
                
                document.getElementById('status').className = 'status connected';
                document.getElementById('status').textContent = '✅ Connected';
                
                if (data.summary) {
                    document.getElementById('geohashTotal').textContent = formatNumber(data.summary.geohashTotalRecords || 0);
                    document.getElementById('h3Total').textContent = formatNumber(data.summary.h3TotalRecords || 0);
                    document.getElementById('geohashSkew').textContent = (data.summary.geohashSkew || 0).toFixed(2);
                    document.getElementById('h3Skew').textContent = (data.summary.h3Skew || 0).toFixed(2);
                }
                
                const tbody = document.getElementById('metricsBody');
                const allMetrics = [...(data.geohash || []), ...(data.h3 || [])];
                if (allMetrics.length > 0) {
                    tbody.innerHTML = allMetrics.map(m => `
                        <tr>
                            <td>${m.partitioner}</td>
                            <td>${m.partition}</td>
                            <td>${m.latitudeBand}</td>
                            <td>${formatNumber(m.recordCount)}</td>
                        </tr>
                    `).join('');
                } else {
                    tbody.innerHTML = '<tr><td colspan="4" style="text-align: center;">No metrics yet</td></tr>';
                }
            } catch (error) {
                document.getElementById('status').className = 'status error';
                document.getElementById('status').textContent = '❌ Error: ' + error.message;
            }
        }
        
        fetchMetrics();
        setInterval(fetchMetrics, 2000);
    </script>
</body>
</html>
""";
    }
    
    private static class MetricsData {
        final Map<String, PartitionMetric> geohashMetrics;
        final Map<String, PartitionMetric> h3Metrics;
        
        MetricsData() {
            this.geohashMetrics = new ConcurrentHashMap<>();
            this.h3Metrics = new ConcurrentHashMap<>();
        }
        
        MetricsData(Map<String, PartitionMetric> geohashMetrics, Map<String, PartitionMetric> h3Metrics) {
            this.geohashMetrics = geohashMetrics;
            this.h3Metrics = h3Metrics;
        }
    }
}
