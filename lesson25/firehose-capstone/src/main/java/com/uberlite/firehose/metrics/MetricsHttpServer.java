package com.uberlite.firehose.metrics;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/**
 * Minimal HTTP metrics endpoint. Uses virtual-thread executor so concurrent
 * dashboard polling never blocks on a shared thread pool.
 * GET /metrics  → JSON payload from MetricsRegistry
 * GET /health   → 200 OK
 */
public class MetricsHttpServer {

    private static final Logger log = LoggerFactory.getLogger(MetricsHttpServer.class);

    private final MetricsRegistry registry;
    private final int port;
    private HttpServer server;

    public MetricsHttpServer(MetricsRegistry registry, int port) {
        this.registry = registry;
        this.port = port;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/metrics", exchange -> {
            var body = registry.toJson().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var os = exchange.getResponseBody()) {
                os.write(body);
            }
        });

        server.createContext("/health", exchange -> {
            byte[] ok = "OK".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, ok.length);
            try (var os = exchange.getResponseBody()) {
                os.write(ok);
            }
        });

        server.createContext("/dashboard", exchange -> {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            String html = """
                <!DOCTYPE html>
                <html><head><meta charset="UTF-8"><title>Firehose Metrics</title>
                <style>
                *{box-sizing:border-box;}
                body{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;max-width:960px;margin:0 auto;padding:0;min-height:100vh;background:linear-gradient(180deg,#0d0d1a 0%,#1a1a2e 40%,#16213e 100%);color:#e8e8e8;}
                .header{background:rgba(15,52,96,0.6);padding:1.25rem 1.5rem;border-bottom:1px solid rgba(255,255,255,0.08);display:flex;align-items:center;justify-content:space-between;}
                .header h1{margin:0;font-size:1.35rem;font-weight:600;color:#fff;letter-spacing:0.02em;}
                .header .badge{background:rgba(0,210,106,0.2);color:#00d26a;font-size:0.7rem;padding:0.25rem 0.5rem;border-radius:4px;font-weight:600;}
                main{padding:1.5rem;}
                .section-title{font-size:0.75rem;text-transform:uppercase;letter-spacing:0.12em;color:rgba(255,255,255,0.5);margin-bottom:1rem;}
                #metrics{display:grid;grid-template-columns:repeat(2,1fr);gap:1rem;}
                .metric{display:flex;flex-direction:column;padding:1.25rem;background:rgba(22,33,62,0.8);border-radius:12px;border:1px solid rgba(255,255,255,0.06);box-shadow:0 4px 24px rgba(0,0,0,0.2);transition:transform 0.15s ease,box-shadow 0.15s ease;}
                .metric:hover{transform:translateY(-2px);box-shadow:0 8px 32px rgba(0,0,0,0.25);}
                .metric span:first-child{font-size:0.8rem;color:rgba(255,255,255,0.7);margin-bottom:0.5rem;}
                .value{font-weight:700;font-size:1.5rem;letter-spacing:0.02em;} .value.ok{color:#00d26a;} .value.warn{color:#ffc107;}
                .value:not(.ok):not(.warn){color:#e94560;}
                .footer{padding:1rem 1.5rem;border-top:1px solid rgba(255,255,255,0.06);font-size:0.75rem;color:rgba(255,255,255,0.4);}
                </style></head><body>
                <header class="header"><h1>Firehose Capstone Dashboard</h1><span class="badge">LIVE</span></header>
                <main><p class="section-title">Key metrics</p><div id="metrics">Loading...</div></main>
                <footer class="footer">Metrics refresh every 1s</footer>
                <script>
                function refresh(){ fetch('/metrics').then(r=>r.json()).then(d=>{
                  document.getElementById('metrics').innerHTML=
                    '<div class="metric"><span>Produce throughput/sec</span><span class="value '+(d.produce_throughput_per_sec>0?'ok':'warn')+'">'+d.produce_throughput_per_sec+'</span></div>'
                    +'<div class="metric"><span>Consume throughput/sec</span><span class="value '+(d.consume_throughput_per_sec>0?'ok':'warn')+'">'+d.consume_throughput_per_sec+'</span></div>'
                    +'<div class="metric"><span>Total produced</span><span class="value">'+d.total_produced+'</span></div>'
                    +'<div class="metric"><span>Total consumed</span><span class="value">'+d.total_consumed+'</span></div>'
                    +'<div class="metric"><span>Total errors</span><span class="value">'+d.total_errors+'</span></div>'
                    +'<div class="metric"><span>P99 produce latency (ms)</span><span class="value">'+d.p99_produce_latency_ms+'</span></div>';
                }).catch(e=>document.getElementById('metrics').innerHTML='Error: '+e);}
                refresh(); setInterval(refresh, 1000);
                </script></body></html>
                """;
            byte[] body = html.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/html; charset=UTF-8");
            exchange.sendResponseHeaders(200, body.length);
            try (var os = exchange.getResponseBody()) { os.write(body); }
        });

        // Virtual-thread executor: each HTTP request gets its own VT, no contention
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        log.info("Metrics server started on http://localhost:{}/metrics", port);
    }

    public void stop() {
        if (server != null) server.stop(0);
    }
}
