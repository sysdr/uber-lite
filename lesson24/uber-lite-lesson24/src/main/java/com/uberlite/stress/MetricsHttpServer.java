package com.uberlite.stress;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Minimal HTTP metrics server — no Micrometer, no Spring, no dependencies.
 * Serves JSON at http://localhost:9090/metrics
 * Serves health check at http://localhost:9090/health
 */
public class MetricsHttpServer {

    record MetricsSnapshot(
            double sendRatePerSec,
            double heapUsedPercent,
            double avgLatencyMs,
            long   totalSent,
            long   totalErrors,
            long   uptimeSeconds
    ) {}

    private final HttpServer server;
    private final AtomicReference<MetricsSnapshot> latest;
    private final long startTime = System.currentTimeMillis();

    public MetricsHttpServer(int port) throws IOException {
        this.latest = new AtomicReference<>(new MetricsSnapshot(0, 0, 0, 0, 0, 0));
        this.server = HttpServer.create(new InetSocketAddress(port), 10);

        server.createContext("/metrics", exchange -> {
            var snap = latest.get();
            var json = String.format("""
                    {
                      "sendRatePerSec":   %.2f,
                      "heapUsedPercent":  %.2f,
                      "avgLatencyMs":     %.3f,
                      "totalSent":        %d,
                      "totalErrors":      %d,
                      "uptimeSeconds":    %d
                    }
                    """,
                    snap.sendRatePerSec(), snap.heapUsedPercent() * 100,
                    snap.avgLatencyMs(), snap.totalSent(), snap.totalErrors(),
                    snap.uptimeSeconds());
            var body = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });

        server.createContext("/health", exchange -> {
            var body = "OK".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.getResponseBody().close();
        });

        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    }

    public void update(double rate, double heap, double latMs, long sent, long errors) {
        latest.set(new MetricsSnapshot(rate, heap, latMs, sent, errors,
                (System.currentTimeMillis() - startTime) / 1000));
    }

    public void start() {
        server.start();
        System.out.printf("==> MetricsHttpServer listening on http://localhost:%d/metrics%n",
                server.getAddress().getPort());
    }

    public void stop() {
        server.stop(2);
    }
}
