package com.uberlite.lesson01.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import com.uberlite.lesson01.kafka.LocationStateProcessor;
import com.uberlite.lesson01.model.Metrics;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Supplier;

public class StateQueryServer {
    private final HttpServer server;
    private final LocationStateProcessor processor;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Supplier<Metrics> kafkaMetricsSupplier;
    private Supplier<Metrics> postgresMetricsSupplier;

    public StateQueryServer(int port, LocationStateProcessor processor) throws IOException {
        this.processor = processor;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        setupRoutes();
    }
    
    public StateQueryServer(int port) throws IOException {
        this.processor = null;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        setupRoutes();
    }
    
    public void setKafkaMetricsSupplier(Supplier<Metrics> supplier) {
        this.kafkaMetricsSupplier = supplier;
    }
    
    public void setPostgresMetricsSupplier(Supplier<Metrics> supplier) {
        this.postgresMetricsSupplier = supplier;
    }
    
    private void setupRoutes() {
        server.createContext("/driver/", exchange -> {
            if (processor == null) {
                var response = "{\"error\":\"Kafka state processor not available\"}";
                exchange.sendResponseHeaders(503, response.length());
                try (var os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
                return;
            }
            var path = exchange.getRequestURI().getPath();
            var driverId = path.substring(path.lastIndexOf('/') + 1);
            
            var location = processor.getDriverLocation(driverId);
            
            if (location != null) {
                var json = objectMapper.writeValueAsString(location);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, json.length());
                try (var os = exchange.getResponseBody()) {
                    os.write(json.getBytes());
                }
            } else {
                var response = "{\"error\":\"Driver not found\"}";
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(404, response.length());
                try (var os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            }
        });
        
        server.createContext("/health", exchange -> {
            var response = "{\"status\":\"ok\"}";
            exchange.sendResponseHeaders(200, response.length());
            try (var os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        });
        
        server.createContext("/metrics", exchange -> {
            try {
                var kafkaMetrics = kafkaMetricsSupplier != null ? kafkaMetricsSupplier.get() : null;
                var postgresMetrics = postgresMetricsSupplier != null ? postgresMetricsSupplier.get() : null;
                
                var response = objectMapper.writeValueAsString(new MetricsResponse(kafkaMetrics, postgresMetrics));
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length());
                try (var os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            } catch (Exception e) {
                var error = "{\"error\":\"" + e.getMessage() + "\"}";
                exchange.sendResponseHeaders(500, error.length());
                try (var os = exchange.getResponseBody()) {
                    os.write(error.getBytes());
                }
            }
        });
        
        server.createContext("/", exchange -> {
            if ("/".equals(exchange.getRequestURI().getPath())) {
                try {
                    var html = getDashboardHtml();
                    var htmlBytes = html.getBytes("UTF-8");
                    exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
                    exchange.sendResponseHeaders(200, htmlBytes.length);
                    try (var os = exchange.getResponseBody()) {
                        os.write(htmlBytes);
                    }
                } catch (Exception e) {
                    var error = "Error: " + e.getMessage();
                    var errorBytes = error.getBytes("UTF-8");
                    exchange.sendResponseHeaders(500, errorBytes.length);
                    try (var os = exchange.getResponseBody()) {
                        os.write(errorBytes);
                    }
                }
            } else {
                exchange.sendResponseHeaders(404, 0);
                exchange.close();
            }
        });
    }
    
    private String getDashboardHtml() {
        return """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Uber-Lite Lesson 01 Dashboard</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
                    .container { max-width: 1200px; margin: 0 auto; }
                    h1 { color: #333; }
                    .metrics-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; margin-top: 20px; }
                    .metric-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
                    .metric-card h2 { margin-top: 0; color: #555; }
                    .metric-item { margin: 10px 0; padding: 10px; background: #f9f9f9; border-radius: 4px; }
                    .metric-label { font-weight: bold; color: #666; }
                    .metric-value { font-size: 24px; color: #2196F3; margin-top: 5px; }
                    .zero { color: #f44336; }
                    .non-zero { color: #4CAF50; }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>🚀 Uber-Lite Lesson 01: Metrics Dashboard</h1>
                    <div class="metrics-grid" id="metrics-grid">
                        <div class="metric-card">
                            <h2>Kafka Metrics</h2>
                            <div class="metric-item">
                                <div class="metric-label">Throughput (events/s)</div>
                                <div class="metric-value" id="kafka-throughput">0</div>
                            </div>
                            <div class="metric-item">
                                <div class="metric-label">p99 Latency (ms)</div>
                                <div class="metric-value" id="kafka-p99">0</div>
                            </div>
                            <div class="metric-item">
                                <div class="metric-label">Success Count</div>
                                <div class="metric-value" id="kafka-success">0</div>
                            </div>
                            <div class="metric-item">
                                <div class="metric-label">Failure Count</div>
                                <div class="metric-value" id="kafka-failure">0</div>
                            </div>
                        </div>
                        <div class="metric-card">
                            <h2>PostgreSQL Metrics</h2>
                            <div class="metric-item">
                                <div class="metric-label">Throughput (events/s)</div>
                                <div class="metric-value" id="postgres-throughput">0</div>
                            </div>
                            <div class="metric-item">
                                <div class="metric-label">p99 Latency (ms)</div>
                                <div class="metric-value" id="postgres-p99">0</div>
                            </div>
                            <div class="metric-item">
                                <div class="metric-label">Success Count</div>
                                <div class="metric-value" id="postgres-success">0</div>
                            </div>
                            <div class="metric-item">
                                <div class="metric-label">Failure Count</div>
                                <div class="metric-value" id="postgres-failure">0</div>
                            </div>
                        </div>
                    </div>
                </div>
                <script>
                    function updateMetrics() {
                        fetch('/metrics')
                            .then(response => response.json())
                            .then(data => {
                                if (data.kafka) {
                                    updateMetric('kafka-throughput', data.kafka.throughput.toFixed(2));
                                    updateMetric('kafka-p99', data.kafka.p99LatencyMs.toFixed(2));
                                    updateMetric('kafka-success', data.kafka.successCount);
                                    updateMetric('kafka-failure', data.kafka.failureCount);
                                }
                                if (data.postgres) {
                                    updateMetric('postgres-throughput', data.postgres.throughput.toFixed(2));
                                    updateMetric('postgres-p99', data.postgres.p99LatencyMs.toFixed(2));
                                    updateMetric('postgres-success', data.postgres.successCount);
                                    updateMetric('postgres-failure', data.postgres.failureCount);
                                }
                            })
                            .catch(error => console.error('Error fetching metrics:', error));
                    }
                    
                    function updateMetric(id, value) {
                        const elem = document.getElementById(id);
                        elem.textContent = value;
                        const numValue = parseFloat(value);
                        elem.className = 'metric-value ' + (numValue === 0 ? 'zero' : 'non-zero');
                    }
                    
                    setInterval(updateMetrics, 1000);
                    updateMetrics();
                </script>
            </body>
            </html>
            """;
    }
    
    private static class MetricsResponse {
        private final Metrics kafka;
        private final Metrics postgres;
        
        public MetricsResponse(Metrics kafka, Metrics postgres) {
            this.kafka = kafka;
            this.postgres = postgres;
        }
        
        public Metrics getKafka() { return kafka; }
        public Metrics getPostgres() { return postgres; }
    }

    public void start() {
        server.start();
        System.out.println("🌐 State query server started on port " + server.getAddress().getPort());
    }

    public void stop() {
        server.stop(0);
    }
}
