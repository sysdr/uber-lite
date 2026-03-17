package com.uberlite.boundary;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/**
 * Dashboard app on port 8080. Proxies metrics from BoundaryBufferingApp (:7072)
 * and serves dashboard.html. Run after BoundaryBufferingApp is running.
 */
public final class MetricsDashboardApp {

    private static final int PORT = 8080;
    private static final String METRICS_URL = "http://localhost:7072/metrics";
    private static final String METRICS_HEALTH_URL = "http://localhost:7072/health";
    private static final HttpClient HTTP = HttpClient.newBuilder().build();

    public static void main(String[] args) throws Exception {
        var server = HttpServer.create(new InetSocketAddress("0.0.0.0", PORT), 0);

        server.createContext("/raw-metrics", ex -> {
            if (!"GET".equals(ex.getRequestMethod())) { ex.sendResponseHeaders(405, -1); return; }
            try {
                var req = HttpRequest.newBuilder().uri(URI.create(METRICS_URL)).GET().build();
                HttpResponse<String> res = HTTP.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                byte[] body = res.body().getBytes(StandardCharsets.UTF_8);
                ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
                ex.sendResponseHeaders(200, body.length);
                try (var os = ex.getResponseBody()) { os.write(body); }
            } catch (Exception e) {
                String msg = "# Error fetching metrics from BoundaryBufferingApp (localhost:7072)\n# " + e.getMessage() + "\n# Ensure BoundaryBufferingApp is running (e.g. ./start.sh).\n";
                byte[] body = msg.getBytes(StandardCharsets.UTF_8);
                ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
                ex.sendResponseHeaders(503, body.length);
                try (var os = ex.getResponseBody()) { os.write(body); }
            }
        });

        server.createContext("/api/source-status", ex -> {
            if (!"GET".equals(ex.getRequestMethod())) { ex.sendResponseHeaders(405, -1); return; }
            boolean up;
            try {
                var req = HttpRequest.newBuilder().uri(URI.create(METRICS_HEALTH_URL)).GET().build();
                HttpResponse<String> res = HTTP.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                up = res.statusCode() == 200;
            } catch (Exception e) { up = false; }
            String json = "{\"metricsSourceUp\":" + up + "}";
            byte[] body = json.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });

        server.createContext("/run-demo", ex -> {
            if (!"POST".equals(ex.getRequestMethod())) { ex.sendResponseHeaders(405, -1); return; }
            Thread.ofVirtual().start(() -> {
                try {
                    var gen = new LoadGenerator("localhost:9092", 100, 20);
                    gen.run();
                } catch (Exception e) { /* ignore */ }
            });
            String msg = "Demo started. Metrics will update shortly.";
            byte[] body = msg.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });

        server.createContext("/health", ex -> {
            byte[] body = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });

        server.createContext("/api/metrics", ex -> {
            if (!"GET".equals(ex.getRequestMethod())) { ex.sendResponseHeaders(405, -1); return; }
            try {
                String json = fetchMetricsJson();
                byte[] body = json.getBytes(StandardCharsets.UTF_8);
                ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
                ex.sendResponseHeaders(200, body.length);
                try (var os = ex.getResponseBody()) { os.write(body); }
            } catch (Exception e) {
                String err = "{\"error\":\"" + e.getMessage() + "\",\"totalProcessed\":0,\"boundaryHits\":0,\"multicastCopies\":0,\"dedupDrops\":0,\"amplificationRatio\":0.0}";
                byte[] body = err.getBytes(StandardCharsets.UTF_8);
                ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
                ex.sendResponseHeaders(200, body.length);
                try (var os = ex.getResponseBody()) { os.write(body); }
            }
        });

        server.createContext("/", ex -> {
            String path = ex.getRequestURI().getPath();
            if (!"/".equals(path) && !"/dashboard".equals(path)) { ex.sendResponseHeaders(404, -1); return; }
            String html;
            try (var in = MetricsDashboardApp.class.getResourceAsStream("/dashboard.html")) {
                html = in != null ? new String(in.readAllBytes(), StandardCharsets.UTF_8)
                    : "<html><body><h1>Dashboard</h1><p>dashboard.html not found</p></body></html>";
            } catch (Exception e) { html = "<html><body><h1>Error</h1><p>" + e.getMessage() + "</p></body></html>"; }
            byte[] body = html.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });
        server.createContext("/dashboard", ex -> { ex.getResponseHeaders().add("Location", "/"); ex.sendResponseHeaders(302, -1); });

        server.start();
        System.out.println("[APP] Dashboard: http://localhost:" + PORT + "/  Metrics from app on :7072. Run main app to see non-zero values.");
        Thread.currentThread().join();
    }

    private static String fetchMetricsJson() throws IOException, InterruptedException {
        var req = HttpRequest.newBuilder().uri(URI.create(METRICS_URL)).GET().build();
        HttpResponse<String> res = HTTP.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (res.statusCode() != 200) throw new IOException("Metrics returned " + res.statusCode());
        long total = parseMetric(res.body(), "boundary_total_processed_total", 0L);
        long hits = parseMetric(res.body(), "boundary_hits_total", 0L);
        long copies = parseMetric(res.body(), "boundary_multicast_copies_total", 0L);
        long drops = parseMetric(res.body(), "dedup_drops_total", 0L);
        double ratio = parseMetricDouble(res.body(), "amplification_ratio", 0.0);
        return String.format(
            "{\"totalProcessed\":%d,\"boundaryHits\":%d,\"multicastCopies\":%d,\"dedupDrops\":%d,\"amplificationRatio\":%.4f}",
            total, hits, copies, drops, ratio);
    }

    private static long parseMetric(String body, String name, long def) {
        Pattern p = Pattern.compile("^" + Pattern.quote(name) + "\\s+([0-9]+)", Pattern.MULTILINE);
        var m = p.matcher(body);
        return m.find() ? Long.parseLong(m.group(1)) : def;
    }

    private static double parseMetricDouble(String body, String name, double def) {
        Pattern p = Pattern.compile("^" + Pattern.quote(name) + "\\s+([0-9.Ee+-]+)", Pattern.MULTILINE);
        var m = p.matcher(body);
        return m.find() ? Double.parseDouble(m.group(1)) : def;
    }
}
