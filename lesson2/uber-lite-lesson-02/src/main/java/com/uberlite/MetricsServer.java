package com.uberlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * HTTP server exposing comprehensive Kafka metrics for dashboard.
 * Endpoints:
 * - /metrics - Prometheus format
 * - /api/metrics - JSON format for dashboard
 * - /dashboard - Dashboard HTML page
 */
public class MetricsServer {
    private static final String TOPIC = "driver-locations";
    private static final String GROUP_ID = "location-processor-v1";
    private static final ObjectMapper mapper = new ObjectMapper();

    // Cache metrics to avoid a full Kafka round-trip on every request
    private static volatile MetricsData cachedMetrics;
    private static volatile String lastError;
    private static final Object cacheLock = new Object();

    public static void main(String[] args) throws IOException {
        var server = HttpServer.create(new InetSocketAddress(8080), 0);

        AdminClient adminClient = createAdminClient();
        startMetricsRefresher(adminClient);

        // Prometheus metrics endpoint
        server.createContext("/metrics", exchange -> {
            try {
                MetricsData metrics = cachedMetrics;
                if (metrics == null) {
                    throw new IllegalStateException(lastError == null
                            ? "Metrics not ready yet"
                            : lastError);
                }

                var response = formatPrometheusMetrics(metrics);

                exchange.getResponseHeaders().add("Content-Type", "text/plain");
                byte[] bytes = response.getBytes();
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            } catch (Exception e) {
                String error = "Error fetching metrics: " + e.getMessage();
                byte[] bytes = error.getBytes();
                exchange.sendResponseHeaders(503, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
        });

        // JSON API endpoint for dashboard
        server.createContext("/api/metrics", exchange -> {
            try {
                MetricsData metrics = cachedMetrics;
                if (metrics == null) {
                    throw new IllegalStateException(lastError == null
                            ? "Metrics not ready yet"
                            : lastError);
                }

                var json = formatJsonMetrics(metrics);

                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                exchange.getResponseHeaders().add("Cache-Control", "no-cache, no-store, must-revalidate");
                exchange.getResponseHeaders().add("Pragma", "no-cache");
                exchange.getResponseHeaders().add("Expires", "0");
                byte[] bytes = json.getBytes();
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            } catch (Exception e) {
                // Log error for debugging
                System.err.println("Error collecting metrics: " + e.getMessage());
                e.printStackTrace();

                ObjectNode error = mapper.createObjectNode();
                String errorMsg = e.getMessage();
                if (errorMsg == null || errorMsg.isEmpty()) {
                    errorMsg = "Unknown error occurred while fetching metrics";
                }
                error.put("error", errorMsg);
                byte[] bytes = error.toString().getBytes();
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                exchange.sendResponseHeaders(503, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
        });
        
        // Dashboard HTML page
        server.createContext("/dashboard", exchange -> {
            var html = getDashboardHtml();
            exchange.getResponseHeaders().add("Content-Type", "text/html");
            byte[] bytes = html.getBytes();
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
        
        // Root redirect to dashboard
        server.createContext("/", exchange -> {
            if ("/".equals(exchange.getRequestURI().getPath())) {
                exchange.getResponseHeaders().add("Location", "/dashboard");
                exchange.sendResponseHeaders(302, 0);
                exchange.close();
            } else {
                exchange.sendResponseHeaders(404, 0);
                exchange.close();
            }
        });
        
        // Cached thread pool so slow Kafka calls never block subsequent HTTP requests
        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        server.start();
        System.out.println("Metrics server started:");
        System.out.println("  Dashboard: http://localhost:8080/dashboard");
        System.out.println("  Metrics API: http://localhost:8080/api/metrics");
        System.out.println("  Prometheus: http://localhost:8080/metrics");
    }

    private static AdminClient createAdminClient() {
        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "metrics-dashboard");
        // Tighten timeouts to avoid long hangs if Kafka is slow/unavailable
        props.put("request.timeout.ms", "3000");
        props.put("default.api.timeout.ms", "3000");
        props.put("connections.max.idle.ms", "10000");
        props.put("retries", "2");
        props.put("reconnect.backoff.ms", "100");
        props.put("reconnect.backoff.max.ms", "500");
        return AdminClient.create(props);
    }

    private static void startMetricsRefresher(AdminClient adminClient) {
        var scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        Runnable refreshTask = () -> {
            try {
                MetricsData metrics = collectMetrics(adminClient);
                synchronized (cacheLock) {
                    cachedMetrics = metrics;
                    lastError = null;
                }
            } catch (Exception e) {
                synchronized (cacheLock) {
                    lastError = e.getMessage();
                    cachedMetrics = null;
                }
                System.err.println("Failed to refresh metrics: " + e.getMessage());
            }
        };

        // Prime immediately then refresh every second (matches dashboard polling)
        refreshTask.run();
        scheduler.scheduleAtFixedRate(refreshTask, 1, 1, java.util.concurrent.TimeUnit.SECONDS);
    }
    
    private static MetricsData collectMetrics(AdminClient adminClient) 
            throws ExecutionException, InterruptedException {
        var groupOffsets = adminClient.listConsumerGroupOffsets(GROUP_ID)
                .partitionsToOffsetAndMetadata().get();
        
        // Get end offsets for all partitions
        Map<TopicPartition, OffsetSpec> partitionsToQuery = new HashMap<>();
        for (TopicPartition tp : groupOffsets.keySet()) {
            partitionsToQuery.put(tp, OffsetSpec.latest());
        }
        
        var endOffsets = adminClient.listOffsets(partitionsToQuery).all().get();
        
        // Calculate lag and totals
        long totalLag = 0;
        long totalCommittedOffset = 0;
        long totalEndOffset = 0;
        int partitionCount = groupOffsets.size();
        
        Map<String, PartitionMetrics> partitionMetrics = new HashMap<>();
        
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : groupOffsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            long committedOffset = entry.getValue().offset();
            long endOffset = endOffsets.get(tp).offset();
            long lag = Math.max(0, endOffset - committedOffset);
            
            totalLag += lag;
            totalCommittedOffset += committedOffset;
            totalEndOffset += endOffset;
            
            partitionMetrics.put(tp.partition() + "", new PartitionMetrics(
                    tp.partition(),
                    committedOffset,
                    endOffset,
                    lag
            ));
        }
        
        return new MetricsData(
                totalCommittedOffset,
                totalEndOffset,
                totalLag,
                partitionCount,
                partitionMetrics
        );
    }
    
    private static String formatPrometheusMetrics(MetricsData metrics) {
        var sb = new StringBuilder("# Kafka Consumer Metrics\n");
        sb.append(String.format("kafka_consumer_committed_offset_total %d\n", 
                metrics.totalCommittedOffset));
        sb.append(String.format("kafka_consumer_end_offset_total %d\n", 
                metrics.totalEndOffset));
        sb.append(String.format("kafka_consumer_lag_total %d\n", metrics.totalLag));
        sb.append(String.format("kafka_topic_partitions %d\n", metrics.partitionCount));
        
        for (PartitionMetrics pm : metrics.partitionMetrics.values()) {
            sb.append(String.format("kafka_consumer_lag{partition=\"%d\"} %d\n", 
                    pm.partition, pm.lag));
            sb.append(String.format("kafka_consumer_committed_offset{partition=\"%d\"} %d\n", 
                    pm.partition, pm.committedOffset));
            sb.append(String.format("kafka_consumer_end_offset{partition=\"%d\"} %d\n", 
                    pm.partition, pm.endOffset));
        }
        
        return sb.toString();
    }
    
    private static String formatJsonMetrics(MetricsData metrics) {
        try {
            ObjectNode root = mapper.createObjectNode();
            root.put("topic", TOPIC);
            root.put("consumerGroup", GROUP_ID);
            root.put("totalCommittedOffset", metrics.totalCommittedOffset);
            root.put("totalEndOffset", metrics.totalEndOffset);
            root.put("totalLag", metrics.totalLag);
            root.put("partitionCount", metrics.partitionCount);
            root.put("timestamp", System.currentTimeMillis());
            
            ArrayNode partitions = root.putArray("partitions");
            for (PartitionMetrics pm : metrics.partitionMetrics.values()) {
                ObjectNode p = partitions.addObject();
                p.put("partition", pm.partition);
                p.put("committedOffset", pm.committedOffset);
                p.put("endOffset", pm.endOffset);
                p.put("lag", pm.lag);
            }
            
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
        } catch (Exception e) {
            return "{\"error\":\"" + e.getMessage() + "\"}";
        }
    }
    
    private static String getDashboardHtml() {
        return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Kafka Metrics Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background-color: #FCEEF5;
            padding: 20px;
            min-height: 100vh;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        h1 {
            color: #2c3e50;
            text-align: center;
            margin-bottom: 30px;
            font-size: 2.5em;
        }
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
            transition: transform 0.2s;
        }
        .metric-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 6px 12px rgba(0,0,0,0.15);
        }
        .metric-label {
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
        }
        .metric-value {
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
            transition: all 0.3s ease;
        }
        .metric-value.updating {
            background: rgba(102, 126, 234, 0.1);
            transform: scale(1.05);
        }
        .metric-subtext {
            color: #999;
            font-size: 0.85em;
            margin-top: 5px;
        }
        .partitions-table {
            background: white;
            border-radius: 12px;
            padding: 25px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            overflow-x: auto;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }
        th {
            background: #f8f9fa;
            font-weight: 600;
            color: #333;
            text-transform: uppercase;
            font-size: 0.85em;
            letter-spacing: 0.5px;
        }
        td {
            color: #555;
        }
        .lag-badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 600;
        }
        .lag-low { background: #d4edda; color: #155724; }
        .lag-medium { background: #fff3cd; color: #856404; }
        .lag-high { background: #f8d7da; color: #721c24; }
        .status {
            text-align: center;
            color: white;
            padding: 10px;
            margin-bottom: 20px;
            border-radius: 8px;
            font-weight: 500;
        }
        .status.connected { background: #28a745; }
        .status.error { background: #dc3545; }
        .refresh-info {
            text-align: center;
            color: #666;
            margin-top: 20px;
            font-size: 0.9em;
            opacity: 0.9;
        }
        .last-updated {
            text-align: center;
            color: #888;
            margin-top: 10px;
            font-size: 0.85em;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚗 Uber-Lite Kafka Metrics Dashboard</h1>
        <div id="status" class="status">Loading...</div>
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-label">Total Committed Offset</div>
                <div class="metric-value" id="totalCommitted">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Total End Offset</div>
                <div class="metric-value" id="totalEnd">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Total Consumer Lag</div>
                <div class="metric-value" id="totalLag">0</div>
                <div class="metric-subtext" id="lagStatus">No lag</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Partitions</div>
                <div class="metric-value" id="partitionCount">0</div>
            </div>
        </div>
        <div class="partitions-table">
            <h2 style="margin-bottom: 20px; color: #333;">Partition Details</h2>
            <table>
                <thead>
                    <tr>
                        <th>Partition</th>
                        <th>Committed Offset</th>
                        <th>End Offset</th>
                        <th>Lag</th>
                    </tr>
                </thead>
                <tbody id="partitionsBody">
                    <tr><td colspan="4" style="text-align: center;">Loading...</td></tr>
                </tbody>
            </table>
        </div>
        <div class="refresh-info">Auto-refreshing every 2 seconds</div>
        <div class="last-updated" id="lastUpdated">Last updated: --</div>
    </div>
    <script>
        function formatNumber(num) {
            return new Intl.NumberFormat().format(num);
        }
        
        function getLagClass(lag) {
            if (lag === 0) return 'lag-low';
            if (lag < 1000) return 'lag-medium';
            return 'lag-high';
        }
        
        function getLagStatus(lag) {
            if (lag === 0) return '✅ No lag';
            if (lag < 1000) return '⚠️ Low lag';
            return '🔴 High lag';
        }
        
        async function fetchMetrics() {
            try {
                const response = await fetch('/api/metrics?t=' + Date.now(), {
                    cache: 'no-store',
                    headers: {
                        'Cache-Control': 'no-cache'
                    }
                });
                
                if (!response.ok) {
                    // Try to get error message from response
                    let errorMsg = 'Failed to fetch metrics';
                    try {
                        const errorData = await response.json();
                        if (errorData.error) {
                            errorMsg = errorData.error;
                        }
                    } catch (e) {
                        errorMsg = `HTTP ${response.status}: ${response.statusText}`;
                    }
                    throw new Error(errorMsg);
                }
                
                const data = await response.json();
                
                // Check if response contains an error
                if (data.error) {
                    throw new Error(data.error);
                }
                
                document.getElementById('status').className = 'status connected';
                document.getElementById('status').textContent = '✅ Connected to Kafka';
                
                // Update metric values with visual feedback
                const metrics = [
                    { id: 'totalCommitted', value: data.totalCommittedOffset || 0 },
                    { id: 'totalEnd', value: data.totalEndOffset || 0 },
                    { id: 'totalLag', value: data.totalLag || 0 },
                    { id: 'partitionCount', value: data.partitionCount || 0 }
                ];
                
                metrics.forEach(({ id, value }) => {
                    const element = document.getElementById(id);
                    const newValue = formatNumber(value);
                    
                    // Always update value to ensure fresh data is displayed
                    // Use innerHTML to force browser re-render
                    element.innerHTML = newValue;
                    
                    // Add visual feedback when updating
                    element.classList.add('updating');
                    setTimeout(() => element.classList.remove('updating'), 300);
                });
                
                document.getElementById('lagStatus').textContent = getLagStatus(data.totalLag || 0);
                
                // Update last updated timestamp
                const now = new Date();
                document.getElementById('lastUpdated').textContent = 
                    'Last updated: ' + now.toLocaleTimeString();
                
                const tbody = document.getElementById('partitionsBody');
                if (data.partitions && data.partitions.length > 0) {
                    tbody.innerHTML = data.partitions.map(p => `
                        <tr>
                            <td>${p.partition}</td>
                            <td>${formatNumber(p.committedOffset)}</td>
                            <td>${formatNumber(p.endOffset)}</td>
                            <td><span class="lag-badge ${getLagClass(p.lag)}">${formatNumber(p.lag)}</span></td>
                        </tr>
                    `).join('');
                } else {
                    tbody.innerHTML = '<tr><td colspan="4" style="text-align: center;">No partitions found</td></tr>';
                }
            } catch (error) {
                document.getElementById('status').className = 'status error';
                document.getElementById('status').textContent = '❌ Error: ' + error.message;
                console.error('Error fetching metrics:', error);
            }
        }
        
        // Initial fetch
        fetchMetrics();
        
        // Auto-refresh every 2 seconds
        setInterval(fetchMetrics, 2000);
    </script>
</body>
</html>
""";
    }
    
    private static class MetricsData {
        final long totalCommittedOffset;
        final long totalEndOffset;
        final long totalLag;
        final int partitionCount;
        final Map<String, PartitionMetrics> partitionMetrics;
        
        MetricsData(long totalCommittedOffset, long totalEndOffset, long totalLag,
                   int partitionCount, Map<String, PartitionMetrics> partitionMetrics) {
            this.totalCommittedOffset = totalCommittedOffset;
            this.totalEndOffset = totalEndOffset;
            this.totalLag = totalLag;
            this.partitionCount = partitionCount;
            this.partitionMetrics = partitionMetrics;
        }
    }
    
    private static class PartitionMetrics {
        final int partition;
        final long committedOffset;
        final long endOffset;
        final long lag;
        
        PartitionMetrics(int partition, long committedOffset, long endOffset, long lag) {
            this.partition = partition;
            this.committedOffset = committedOffset;
            this.endOffset = endOffset;
            this.lag = lag;
        }
    }
}
