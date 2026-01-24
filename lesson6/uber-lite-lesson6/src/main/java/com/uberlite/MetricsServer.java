package com.uberlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static com.uberlite.Models.*;

public class MetricsServer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final ObjectMapper mapper = new ObjectMapper();
    
    private static final AtomicLong driverLocationsIndexed = new AtomicLong(0);
    private static final AtomicLong matchRequestsProcessed = new AtomicLong(0);
    private static final AtomicLong matchesFound = new AtomicLong(0);
    private static final AtomicLong totalMatchLatencyMs = new AtomicLong(0);
    private static final AtomicLong totalDriversFound = new AtomicLong(0);
    private static volatile String lastError;
    
    public static void main(String[] args) throws IOException {
        var server = HttpServer.create(new InetSocketAddress("0.0.0.0", 8080), 0);
        
        // Start metrics consumers
        startMetricsConsumers();
        
        // JSON API endpoint
        server.createContext("/api/metrics", exchange -> {
            try {
                var json = formatJsonMetrics();
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
    
    private static void startMetricsConsumers() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "metrics-dashboard-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        
        // Consumer for match results
        new Thread(() -> {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList("match-results"));
                
                while (true) {
                    var records = consumer.poll(Duration.ofMillis(100));
                    for (var record : records) {
                        try {
                            MatchResult result = mapper.readValue(record.value(), MatchResult.class);
                            matchRequestsProcessed.incrementAndGet();
                            matchesFound.incrementAndGet();
                            totalMatchLatencyMs.addAndGet(result.matchLatencyMs());
                            totalDriversFound.incrementAndGet();
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
        }).start();
        
        // Consumer for driver locations (to track indexed count)
        Properties driverProps = new Properties();
        driverProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        driverProps.put(ConsumerConfig.GROUP_ID_CONFIG, "metrics-driver-consumer");
        driverProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        driverProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        driverProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        driverProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        
        new Thread(() -> {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(driverProps)) {
                consumer.subscribe(Collections.singletonList("driver-locations"));
                
                while (true) {
                    var records = consumer.poll(Duration.ofMillis(100));
                    for (var record : records) {
                        try {
                            driverLocationsIndexed.incrementAndGet();
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
        }).start();
    }
    
    private static String formatJsonMetrics() throws Exception {
        ObjectNode root = mapper.createObjectNode();
        root.put("timestamp", System.currentTimeMillis());
        
        long indexed = driverLocationsIndexed.get();
        long requests = matchRequestsProcessed.get();
        long matches = matchesFound.get();
        long totalLatency = totalMatchLatencyMs.get();
        double avgLatency = requests > 0 ? (double) totalLatency / requests : 0.0;
        
        root.put("driverLocationsIndexed", indexed);
        root.put("matchRequestsProcessed", requests);
        root.put("matchesFound", matches);
        root.put("totalMatchLatencyMs", totalLatency);
        root.put("averageMatchLatencyMs", Math.round(avgLatency * 100.0) / 100.0); // Round to 2 decimal places
        root.put("totalDriversFound", totalDriversFound.get());
        
        if (lastError != null) {
            root.put("lastError", lastError);
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
    <title>H3 Spatial Index Dashboard</title>
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
        .status { text-align: center; color: white; padding: 10px; margin-bottom: 20px; border-radius: 8px; }
        .status.connected { background: #28a745; }
        .status.error { background: #dc3545; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🗺️ H3 Spatial Index Dashboard</h1>
        <div id="status" class="status">Loading...</div>
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-label">Driver Locations Indexed</div>
                <div class="metric-value" id="driverLocationsIndexed">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Match Requests Processed</div>
                <div class="metric-value" id="matchRequestsProcessed">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Matches Found</div>
                <div class="metric-value" id="matchesFound">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Average Match Latency (ms)</div>
                <div class="metric-value" id="averageMatchLatencyMs">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Total Match Latency (ms)</div>
                <div class="metric-value" id="totalMatchLatencyMs">0</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Total Drivers Found</div>
                <div class="metric-value" id="totalDriversFound">0</div>
            </div>
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
                
                document.getElementById('driverLocationsIndexed').textContent = formatNumber(data.driverLocationsIndexed || 0);
                document.getElementById('matchRequestsProcessed').textContent = formatNumber(data.matchRequestsProcessed || 0);
                document.getElementById('matchesFound').textContent = formatNumber(data.matchesFound || 0);
                document.getElementById('averageMatchLatencyMs').textContent = formatNumber(data.averageMatchLatencyMs || 0);
                document.getElementById('totalMatchLatencyMs').textContent = formatNumber(data.totalMatchLatencyMs || 0);
                document.getElementById('totalDriversFound').textContent = formatNumber(data.totalDriversFound || 0);
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
}
