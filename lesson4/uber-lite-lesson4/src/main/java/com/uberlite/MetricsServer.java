package com.uberlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * HTTP server exposing Kafka cluster metrics for Geospatial Indexing dashboard.
 * Endpoints:
 * - /metrics - Prometheus format
 * - /api/metrics - JSON format for dashboard
 * - /dashboard - Dashboard HTML page
 */
public class MetricsServer {
    private static final String[] TOPICS = {"driver-locations", "driver-locations-by-h3"};
    private static final ObjectMapper mapper = new ObjectMapper();

    // Cache metrics to avoid a full Kafka round-trip on every request
    private static volatile MetricsData cachedMetrics;
    private static volatile String lastError;
    private static final Object cacheLock = new Object();

    public static void main(String[] args) throws IOException {
        var server = HttpServer.create(new InetSocketAddress("0.0.0.0", 8080), 0);

        AdminClient adminClient = createAdminClient();

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

                exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4");
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
                
                // Include error in response if present
                if (lastError != null && metrics.brokerCount == 0 && metrics.partitionCount == 0) {
                    var jsonNode = mapper.readTree(json);
                    if (jsonNode.isObject()) {
                        ((ObjectNode) jsonNode).put("error", lastError);
                        json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode);
                    }
                }

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
                e.printStackTrace();

                ObjectNode error = mapper.createObjectNode();
                String errorMsg = e.getMessage();
                if (errorMsg == null || errorMsg.isEmpty()) {
                    errorMsg = "Unknown error occurred while fetching metrics";
                }
                error.put("error", errorMsg);
                error.put("topic", TOPICS[0]);
                error.put("totalEndOffset", 0);
                error.put("partitionCount", 0);
                error.put("brokerCount", 0);
                error.put("timestamp", System.currentTimeMillis());
                error.putArray("partitions");
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
        
        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        server.start();
        
        int port = server.getAddress().getPort();
        System.out.println("Metrics server started on port " + port);
        System.out.println("  Dashboard: http://localhost:" + port + "/dashboard");
        System.out.println("  Metrics API: http://localhost:" + port + "/api/metrics");
        System.out.println("  Prometheus: http://localhost:" + port + "/metrics");
        
        // Initialize with empty metrics so dashboard doesn't show errors
        synchronized (cacheLock) {
            cachedMetrics = new MetricsData(0, 0, 0, new HashMap<>());
        }
        
        // Start metrics collection in background thread
        new Thread(() -> {
            startMetricsRefresher(adminClient);
        }).start();
    }

    private static AdminClient createAdminClient() {
        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "metrics-dashboard");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");
        props.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "300000");
        props.put(AdminClientConfig.RETRIES_CONFIG, "5");
        props.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
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
                long now = System.currentTimeMillis();
                if (now % 20000 < 2000) {
                    System.out.println("[METRICS] Refreshed: brokers=" + metrics.brokerCount + 
                                     ", partitions=" + metrics.partitionCount + 
                                     ", totalOffset=" + metrics.totalEndOffset);
                }
            } catch (Exception e) {
                String errorMsg = e.getMessage();
                if (errorMsg == null) {
                    errorMsg = e.getClass().getSimpleName();
                }
                synchronized (cacheLock) {
                    lastError = errorMsg;
                    if (cachedMetrics == null) {
                        cachedMetrics = new MetricsData(0, 0, 0, new HashMap<>());
                    }
                }
                long now = System.currentTimeMillis();
                if (now % 10000 < 1000) {
                    System.err.println("[METRICS ERROR] " + errorMsg);
                }
            }
        };

        refreshTask.run();
        scheduler.scheduleAtFixedRate(refreshTask, 2, 2, java.util.concurrent.TimeUnit.SECONDS);
    }
    
    private static MetricsData collectMetrics(AdminClient adminClient) throws Exception {
        int brokerCount = 0;
        try {
            var nodesFuture = adminClient.describeCluster().nodes();
            var nodes = nodesFuture.get(30, java.util.concurrent.TimeUnit.SECONDS);
            brokerCount = nodes.size();
            if (brokerCount == 0) {
                throw new Exception("No Kafka brokers available");
            }
        } catch (java.util.concurrent.TimeoutException e) {
            throw new Exception("Kafka connection timeout - check if Kafka is running on localhost:9092");
        } catch (Exception e) {
            String errorMsg = e.getMessage();
            if (errorMsg == null || errorMsg.isEmpty()) {
                errorMsg = e.getClass().getSimpleName();
            }
            throw new Exception("Failed to connect to Kafka: " + errorMsg);
        }
        
        // Collect metrics from all topics
        Set<String> topics;
        try {
            var topicsFuture = adminClient.listTopics().names();
            topics = topicsFuture.get(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            return new MetricsData(0, 0, brokerCount, new HashMap<>());
        }
        
        long totalEndOffset = 0;
        int totalPartitionCount = 0;
        Map<String, PartitionMetrics> allPartitionMetrics = new HashMap<>();
        
        for (String topicName : TOPICS) {
            if (!topics.contains(topicName)) {
                continue;
            }
            
            org.apache.kafka.clients.admin.TopicDescription topic;
            try {
                var topicDescFuture = adminClient.describeTopics(List.of(topicName)).allTopicNames();
                var topicDesc = topicDescFuture.get(30, java.util.concurrent.TimeUnit.SECONDS);
                topic = topicDesc.get(topicName);
            } catch (Exception e) {
                continue;
            }
            
            if (topic == null || topic.partitions() == null || topic.partitions().isEmpty()) {
                continue;
            }
            
            totalPartitionCount += topic.partitions().size();
            
            Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> partitionsToQuery = new HashMap<>();
            for (TopicPartitionInfo partition : topic.partitions()) {
                if (partition.leader() != null) {
                    partitionsToQuery.put(
                        new org.apache.kafka.common.TopicPartition(topicName, partition.partition()),
                        org.apache.kafka.clients.admin.OffsetSpec.latest()
                    );
                }
            }
            
            if (partitionsToQuery.isEmpty()) {
                for (TopicPartitionInfo partition : topic.partitions()) {
                    var leader = partition.leader();
                    var replicas = partition.replicas();
                    var isr = partition.isr();
                    String key = topicName + "-" + partition.partition();
                    allPartitionMetrics.put(key, new PartitionMetrics(
                        partition.partition(),
                        0,
                        leader != null ? leader.id() : -1,
                        replicas.size(),
                        isr.size(),
                        isr.size() < replicas.size()
                    ));
                }
                continue;
            }
            
            Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> endOffsets;
            try {
                var offsetsFuture = adminClient.listOffsets(partitionsToQuery).all();
                endOffsets = offsetsFuture.get(5, java.util.concurrent.TimeUnit.SECONDS);
            } catch (Exception e) {
                for (TopicPartitionInfo partition : topic.partitions()) {
                    var leader = partition.leader();
                    var replicas = partition.replicas();
                    var isr = partition.isr();
                    String key = topicName + "-" + partition.partition();
                    allPartitionMetrics.put(key, new PartitionMetrics(
                        partition.partition(),
                        0,
                        leader != null ? leader.id() : -1,
                        replicas.size(),
                        isr.size(),
                        isr.size() < replicas.size()
                    ));
                }
                continue;
            }
            
            for (TopicPartitionInfo partition : topic.partitions()) {
                var tp = new org.apache.kafka.common.TopicPartition(topicName, partition.partition());
                
                long endOffset = 0;
                if (endOffsets.containsKey(tp)) {
                    try {
                        endOffset = endOffsets.get(tp).offset();
                        totalEndOffset += endOffset;
                    } catch (Exception e) {
                        // Ignore
                    }
                }
                
                var leader = partition.leader();
                var replicas = partition.replicas();
                var isr = partition.isr();
                
                String key = topicName + "-" + partition.partition();
                allPartitionMetrics.put(key, new PartitionMetrics(
                    partition.partition(),
                    endOffset,
                    leader != null ? leader.id() : -1,
                    replicas.size(),
                    isr.size(),
                    isr.size() < replicas.size()
                ));
            }
        }
        
        return new MetricsData(totalEndOffset, totalPartitionCount, brokerCount, allPartitionMetrics);
    }
    
    private static String formatPrometheusMetrics(MetricsData metrics) {
        var sb = new StringBuilder("# Kafka Cluster Metrics\n");
        sb.append(String.format("kafka_topic_end_offset_total %d\n", metrics.totalEndOffset));
        sb.append(String.format("kafka_topic_partitions %d\n", metrics.partitionCount));
        sb.append(String.format("kafka_cluster_brokers %d\n", metrics.brokerCount));
        
        for (PartitionMetrics pm : metrics.partitionMetrics.values()) {
            sb.append(String.format("kafka_partition_end_offset{partition=\"%d\"} %d\n", 
                    pm.partition, pm.endOffset));
            sb.append(String.format("kafka_partition_leader{partition=\"%d\"} %d\n", 
                    pm.partition, pm.leaderId));
            sb.append(String.format("kafka_partition_replicas{partition=\"%d\"} %d\n", 
                    pm.partition, pm.replicaCount));
            sb.append(String.format("kafka_partition_isr{partition=\"%d\"} %d\n", 
                    pm.partition, pm.isrCount));
            sb.append(String.format("kafka_partition_under_replicated{partition=\"%d\"} %d\n", 
                    pm.partition, pm.underReplicated ? 1 : 0));
        }
        
        return sb.toString();
    }
    
    private static String formatJsonMetrics(MetricsData metrics) {
        try {
            ObjectNode root = mapper.createObjectNode();
            root.put("topic", String.join(", ", TOPICS));
            root.put("totalEndOffset", metrics.totalEndOffset);
            root.put("partitionCount", metrics.partitionCount);
            root.put("brokerCount", metrics.brokerCount);
            root.put("timestamp", System.currentTimeMillis());
            
            ArrayNode partitions = root.putArray("partitions");
            for (PartitionMetrics pm : metrics.partitionMetrics.values()) {
                ObjectNode p = partitions.addObject();
                p.put("partition", pm.partition);
                p.put("endOffset", pm.endOffset);
                p.put("leaderId", pm.leaderId);
                p.put("replicaCount", pm.replicaCount);
                p.put("isrCount", pm.isrCount);
                p.put("underReplicated", pm.underReplicated);
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
    <title>Geospatial Indexing with H3 - Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background-color: #F0F7FF;
            padding: 20px;
            min-height: 100vh;
        }
        .container {
            max-width: 1400px;
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
        .status-badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 600;
        }
        .status-ok { background: #d4edda; color: #155724; }
        .status-warn { background: #fff3cd; color: #856404; }
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
        <h1>🗺️ Geospatial Indexing with H3 Dashboard</h1>
        <div id="status" class="status">Loading...</div>
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-label">Total Messages</div>
                <div class="metric-value" id="totalEnd">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Partitions</div>
                <div class="metric-value" id="partitionCount">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Brokers</div>
                <div class="metric-value" id="brokerCount">0</div>
            </div>
        </div>
        <div class="partitions-table">
            <h2 style="margin-bottom: 20px; color: #333;">Partition Details</h2>
            <table>
                <thead>
                    <tr>
                        <th>Partition</th>
                        <th>End Offset</th>
                        <th>Leader</th>
                        <th>Replicas</th>
                        <th>ISR</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody id="partitionsBody">
                    <tr><td colspan="6" style="text-align: center;">Loading...</td></tr>
                </tbody>
            </table>
        </div>
        <div class="refresh-info">
            Auto-refreshing every 2 seconds 
            <span id="refreshIndicator" style="display: inline-block; margin-left: 5px; transition: opacity 0.3s;">●</span>
        </div>
        <div class="last-updated" id="lastUpdated">Last updated: --</div>
    </div>
    <script>
        console.log("[DASHBOARD LOADED]");
        
        function formatNumber(num) {
            return new Intl.NumberFormat().format(num);
        }
        
        function fetchWithTimeout(url, options, timeout = 5000) {
            console.log('[FETCH] Fetching:', url);
            return Promise.race([
                fetch(url, options),
                new Promise((_, reject) => 
                    setTimeout(() => reject(new Error('Request timeout')), timeout)
                )
            ]);
        }
        
        let loadingCleared = false;
        const loadingTimeout = setTimeout(() => {
            const statusEl = document.getElementById('status');
            if (statusEl && statusEl.textContent === 'Loading...') {
                console.log('[TIMEOUT] Loading status timeout - clearing after 1s');
                statusEl.className = 'status error';
                statusEl.textContent = '⏳ Loading metrics...';
                loadingCleared = true;
            }
        }, 1000);
        
        let lastTimestamp = 0;
        
        async function fetchMetrics() {
            const statusEl = document.getElementById('status');
            const lastUpdatedEl = document.getElementById('lastUpdated');
            
            try {
                const url = '/api/metrics?t=' + Date.now();
                console.log('[FETCH] Starting fetchMetrics at', new Date().toISOString());
                
                const response = await fetchWithTimeout(url, {
                    cache: 'no-store',
                    headers: {
                        'Cache-Control': 'no-cache'
                    }
                }, 5000);
                
                console.log('[FETCH] Response received:', response.status);
                
                if (!response.ok) {
                    let errorMsg = 'Failed to fetch metrics';
                    try {
                        const contentType = response.headers.get('Content-Type') || '';
                        if (contentType.includes('application/json')) {
                            const errorData = await response.json();
                            if (errorData.error) {
                                errorMsg = errorData.error;
                            }
                        } else {
                            errorMsg = `HTTP ${response.status}: ${response.statusText}`;
                        }
                    } catch (e) {
                        errorMsg = `HTTP ${response.status}: ${response.statusText}`;
                    }
                    throw new Error(errorMsg);
                }
                
                const text = await response.text();
                if (!text || text.trim().length === 0) {
                    throw new Error('Empty response from server');
                }
                const data = JSON.parse(text);
                
                // Always update timestamp to track refresh
                const isNewData = data.timestamp && data.timestamp !== lastTimestamp;
                if (isNewData) {
                    lastTimestamp = data.timestamp;
                    console.log('[FETCH] New data received (timestamp:', data.timestamp, ')');
                } else if (data.timestamp) {
                    // Even if timestamp is same, log that we're refreshing
                    console.log('[FETCH] Refreshing with same timestamp:', data.timestamp);
                }
                
                console.log('[FETCH] Data received:', {
                    totalEndOffset: data.totalEndOffset,
                    partitionCount: data.partitionCount,
                    brokerCount: data.brokerCount,
                    partitionsLength: data.partitions ? data.partitions.length : 0,
                    timestamp: data.timestamp
                });
                
                if (data.error) {
                    console.warn('[FETCH] Error in response:', data.error);
                    statusEl.className = 'status error';
                    statusEl.textContent = '⚠️ Warning: ' + data.error;
                }
                
                clearTimeout(loadingTimeout);
                loadingCleared = true;
                
                statusEl.className = 'status connected';
                statusEl.textContent = '✅ Connected to Kafka Cluster';
                
                const metrics = [
                    { id: 'totalEnd', value: data.totalEndOffset || 0 },
                    { id: 'partitionCount', value: data.partitionCount || 0 },
                    { id: 'brokerCount', value: data.brokerCount || 0 }
                ];
                
                // ALWAYS update metrics to show refresh, even if values are the same
                metrics.forEach(({ id, value }) => {
                    const element = document.getElementById(id);
                    if (element) {
                        const newValue = formatNumber(value);
                        const oldValue = element.textContent.trim();
                        // Force update by clearing and setting - this ensures visual refresh
                        element.textContent = '';
                        // Use requestAnimationFrame to ensure DOM update
                        requestAnimationFrame(() => {
                            element.innerHTML = newValue;
                            // Add visual feedback for update
                            element.classList.add('updating');
                            setTimeout(() => element.classList.remove('updating'), 300);
                        });
                        // Log if value actually changed
                        if (oldValue !== newValue) {
                            console.log(`[UPDATE] ${id}: ${oldValue} → ${newValue}`);
                        } else {
                            console.log(`[REFRESH] ${id}: ${newValue} (no change, but UI updated)`);
                        }
                    }
                });
                
                // ALWAYS update last updated time to show refresh activity - use current time
                const now = new Date();
                const timeStr = now.toLocaleTimeString('en-US', {hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit', fractionalSecondDigits: 1});
                lastUpdatedEl.textContent = 'Last updated: ' + timeStr;
                // Force a visual update by toggling a class
                lastUpdatedEl.style.opacity = '0.7';
                setTimeout(() => { lastUpdatedEl.style.opacity = '1'; }, 100);
                
                const tbody = document.getElementById('partitionsBody');
                if (data.partitions && data.partitions.length > 0) {
                    const newTableHtml = data.partitions.map(p => `
                        <tr>
                            <td>${p.partition}</td>
                            <td>${formatNumber(p.endOffset)}</td>
                            <td>${p.leaderId}</td>
                            <td>${p.replicaCount}</td>
                            <td>${p.isrCount}</td>
                            <td><span class="status-badge ${p.underReplicated ? 'status-warn' : 'status-ok'}">${p.underReplicated ? '⚠️ Under-replicated' : '✅ Healthy'}</span></td>
                        </tr>
                    `).join('');
                    tbody.innerHTML = newTableHtml;
                } else {
                    tbody.innerHTML = '<tr><td colspan="6" style="text-align: center;">No partitions found</td></tr>';
                }
                
                console.log('[FETCH] Metrics updated successfully');
            } catch (error) {
                console.error('[ERROR] fetchMetrics failed:', error);
                
                clearTimeout(loadingTimeout);
                loadingCleared = true;
                
                statusEl.className = 'status error';
                statusEl.textContent = '❌ Error: ' + error.message;
                
                const now = new Date();
                lastUpdatedEl.textContent = 'Last updated: ' + now.toLocaleTimeString() + ' (Error)';
                
                const tbody = document.getElementById('partitionsBody');
                tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: #d32f2f;">Error loading partitions: ' + error.message + '</td></tr>';
            }
        }
        
        setTimeout(() => {
            console.log('[INIT] Starting initial metrics fetch');
            fetchMetrics();
        }, 100);
        
        const intervalId = setInterval(() => {
            const pollTime = new Date().toLocaleTimeString('en-US', {hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit', fractionalSecondDigits: 1});
            console.log('[POLL] Auto-refresh triggered at', pollTime);
            fetchMetrics();
        }, 2000);
        
        // Add pulsing animation to refresh indicator
        setInterval(() => {
            const refreshIndicator = document.getElementById('refreshIndicator');
            if (refreshIndicator) {
                refreshIndicator.style.opacity = refreshIndicator.style.opacity === '0.3' ? '1' : '0.3';
            }
        }, 1000);
        
        window.addEventListener('beforeunload', () => {
            clearInterval(intervalId);
            clearTimeout(loadingTimeout);
        });
    </script>
</body>
</html>
""";
    }
    
    private static class MetricsData {
        final long totalEndOffset;
        final int partitionCount;
        final int brokerCount;
        final Map<String, PartitionMetrics> partitionMetrics;
        
        MetricsData(long totalEndOffset, int partitionCount, int brokerCount,
                   Map<String, PartitionMetrics> partitionMetrics) {
            this.totalEndOffset = totalEndOffset;
            this.partitionCount = partitionCount;
            this.brokerCount = brokerCount;
            this.partitionMetrics = partitionMetrics;
        }
    }
    
    private static class PartitionMetrics {
        final int partition;
        final long endOffset;
        final int leaderId;
        final int replicaCount;
        final int isrCount;
        final boolean underReplicated;
        
        PartitionMetrics(int partition, long endOffset, int leaderId, 
                         int replicaCount, int isrCount, boolean underReplicated) {
            this.partition = partition;
            this.endOffset = endOffset;
            this.leaderId = leaderId;
            this.replicaCount = replicaCount;
            this.isrCount = isrCount;
            this.underReplicated = underReplicated;
        }
    }
}

