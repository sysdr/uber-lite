package com.uberlite.boundary;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Lightweight HTTP metrics endpoint on port 7072.
 * Returns plain-text metrics compatible with Prometheus text format.
 *
 * Endpoints:
 *   GET /metrics  → Prometheus-compatible text
 *   GET /health   → 200 OK
 */
public final class MetricsServer {

    private static final Logger log = LoggerFactory.getLogger(MetricsServer.class);
    private static final int PORT = 7072;

    private final HttpServer server;
    private final BoundaryMetrics metrics;

    public MetricsServer(BoundaryMetrics metrics) throws IOException {
        this.metrics = metrics;
        this.server = HttpServer.create(new InetSocketAddress("0.0.0.0", PORT), 10);

        server.createContext("/metrics", exchange -> {
            String body = buildMetrics();
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(200, bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });

        server.createContext("/health", exchange -> {
            byte[] ok = "OK\n".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, ok.length);
            try (var out = exchange.getResponseBody()) {
                out.write(ok);
            }
        });
    }

    private String buildMetrics() {
        long total    = metrics.totalProcessed.get();
        long hits     = metrics.boundaryHits.get();
        long copies   = metrics.multicastCopies.get();
        long drops    = metrics.dedupDrops.get();
        double ratio  = metrics.amplificationRatio();

        return """
            # HELP boundary_total_processed_total Total driver location events processed
            # TYPE boundary_total_processed_total counter
            boundary_total_processed_total %d
            # HELP boundary_hits_total Events near an H3 Res-5 boundary
            # TYPE boundary_hits_total counter
            boundary_hits_total %d
            # HELP boundary_multicast_copies_total Extra copies emitted due to boundary proximity
            # TYPE boundary_multicast_copies_total counter
            boundary_multicast_copies_total %d
            # HELP dedup_drops_total Duplicate records dropped by DedupProcessor
            # TYPE dedup_drops_total counter
            dedup_drops_total %d
            # HELP amplification_ratio Ratio of multicast copies to total (target < 0.15)
            # TYPE amplification_ratio gauge
            amplification_ratio %.4f
            """.formatted(total, hits, copies, drops, ratio);
    }

    public void start() {
        server.start();
        log.info("MetricsServer started on http://localhost:{}/metrics", PORT);
    }

    public void stop() {
        server.stop(0);
    }
}
