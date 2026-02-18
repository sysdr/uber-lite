package com.uberlite.locality;

import com.uberlite.locality.producer.DataLocalitySimulator;
import com.uberlite.locality.topology.MatchingTopology;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * Main entry point for the Data Locality Matching application.
 *
 * Starts:
 *   1. Kafka Streams application with MatchingTopology
 *   2. HTTP metrics endpoint on :8080/metrics (virtual thread executor)
 *
 * CRITICAL: StreamsConfig.NUM_STREAM_THREADS_CONFIG = 4 means Kafka Streams
 * creates 4 StreamThreads. With 12 partitions across 2 topics (24 tasks total),
 * each StreamThread owns 3 tasks. Each task processes a pair of co-partitioned
 * driver-locations[N] + rider-requests[N] — this is data locality in action.
 */
public final class LocalityMatchingApp {

    private static final Logger LOG = LoggerFactory.getLogger(LocalityMatchingApp.class);

    public static void main(String[] args) throws Exception {
        var streams = new KafkaStreams(MatchingTopology.build(), buildStreamsConfig());

        streams.setUncaughtExceptionHandler(exception -> {
            LOG.error("Uncaught StreamThread exception", exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        // Graceful shutdown on SIGTERM / Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received. Closing Kafka Streams...");
            streams.close();
        }, "streams-shutdown-hook"));

        streams.start();
        LOG.info("KafkaStreams started. State: {}", streams.state());
        int initialTaskCount = streams.metadataForLocalThreads().stream()
            .mapToInt(t -> t.activeTasks().size()).sum();
        LOG.info("TaskManager: {} active tasks", initialTaskCount);

        // ── Metrics HTTP Server (Virtual Threads) ────────────────────────
        var server = HttpServer.create(new InetSocketAddress(8080), 0);

        // Dashboard: HTML at / and /dashboard for human-readable expected output
        server.createContext("/", exchange -> {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            String path = exchange.getRequestURI().getPath();
            if (!"/".equals(path) && !"/dashboard".equals(path)) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }
            var summary = computeSummaryMetrics(streams);
            String html = buildDashboardHtml(summary);
            byte[] response = html.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
            exchange.sendResponseHeaders(200, response.length);
            try (var os = exchange.getResponseBody()) { os.write(response); }
        });

        server.createContext("/metrics", exchange -> {
            var state = streams.state();
            int activeTaskCount = streams.metadataForLocalThreads().stream()
                .mapToInt(t -> t.activeTasks().size()).sum();
            var summary = computeSummaryMetrics(streams);

            var sb = new StringBuilder();
            sb.append("# Lesson 26 Data Locality Metrics\n");
            sb.append("# Dashboard: http://localhost:8080/ or http://localhost:8080/dashboard\n");
            sb.append("# Run ./demo.sh to generate traffic; rates show current throughput, totals are cumulative.\n");
            sb.append("streams_state{state=\"").append(state).append("\"} 1\n");
            sb.append("streams_active_tasks ").append(activeTaskCount).append("\n");
            sb.append("streams_records_consumed_total ").append(summary.consumedTotal).append("\n");
            sb.append("streams_records_produced_total ").append(summary.producedTotal).append("\n");
            sb.append("streams_process_rate_total ").append(String.format("%.2f", summary.processRate)).append("\n");
            sb.append("streams_commit_rate_total ").append(String.format("%.2f", summary.commitRate)).append("\n");

            try {
                var storeMetrics = streams.metrics();
                storeMetrics.forEach((metricName, metric) -> {
                    var name = metricName.name();
                    if (name != null && (name.contains("process-rate") || name.contains("rocksdb")
                        || name.contains("commit-rate") || name.contains("records-consumed-total")
                        || name.contains("records-produced-total"))) {
                        Object v = metric.metricValue();
                        sb.append("kafka_streams_").append(name.replace('-', '_')).append(" ");
                        if (v instanceof Number) sb.append(v);
                        else sb.append(v != null ? v.toString() : "0");
                        sb.append("\n");
                    }
                });
            } catch (Exception e) {
                sb.append("# Error: ").append(e.getMessage()).append("\n");
            }

            byte[] response = sb.toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(200, response.length);
            try (var os = exchange.getResponseBody()) { os.write(response); }
        });

        server.createContext("/dashboard.css", exchange -> {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            try (var in = LocalityMatchingApp.class.getResourceAsStream("/dashboard.css")) {
                if (in == null) {
                    exchange.sendResponseHeaders(404, -1);
                    return;
                }
                byte[] css = in.readAllBytes();
                exchange.getResponseHeaders().set("Content-Type", "text/css; charset=utf-8");
                exchange.sendResponseHeaders(200, css.length);
                try (var os = exchange.getResponseBody()) { os.write(css); }
            }
        });

        server.createContext("/health", exchange -> {
            var status = streams.state().isRunningOrRebalancing() ? "UP" : "DOWN";
            byte[] response = ("{\"status\":\"" + status + "\"}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (var os = exchange.getResponseBody()) {
                os.write(response);
            }
        });

        server.createContext("/api/metrics", exchange -> {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            var s = computeSummaryMetrics(streams);
            String json = String.format(
                "{\"state\":\"%s\",\"activeTasks\":%d,\"consumedTotal\":%d,\"producedTotal\":%d,\"processRate\":%.2f,\"commitRate\":%.2f}",
                s.state(), s.activeTasks(), s.consumedTotal(), s.producedTotal(), s.processRate(), s.commitRate());
            byte[] response = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.getResponseHeaders().set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0");
            exchange.getResponseHeaders().set("Pragma", "no-cache");
            exchange.sendResponseHeaders(200, response.length);
            try (var os = exchange.getResponseBody()) { os.write(response); }
        });

        server.createContext("/api/debug-metrics", exchange -> {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            var sb = new StringBuilder();
            sb.append("{\"metrics\":[");
            String[] keywords = { "record", "consumed", "produced", "process", "commit", "stream" };
            try {
                var first = true;
                for (var entry : streams.metrics().entrySet()) {
                    var key = entry.getKey();
                    var name = key.name() != null ? key.name() : "";
                    var group = key.group() != null ? key.group() : "";
                    String combined = (name + " " + group).toLowerCase();
                    boolean include = false;
                    for (String kw : keywords) {
                        if (combined.contains(kw)) { include = true; break; }
                    }
                    if (!include) continue;
                    Object v = entry.getValue().metricValue();
                    String valStr = (v instanceof Number) ? v.toString() : (v != null ? "\"" + v.toString().replace("\"", "\\\"") + "\"" : "0");
                    if (!first) sb.append(",");
                    first = false;
                    sb.append("{\"name\":\"").append(name.replace("\"", "\\\"")).append("\",\"group\":\"").append(group.replace("\"", "\\\"")).append("\",\"value\":").append(valStr).append("}");
                }
            } catch (Exception e) {
                sb.append("],\"error\":\"").append(e.getMessage().replace("\"", "\\\"")).append("\"}");
                byte[] resp = sb.toString().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
                exchange.sendResponseHeaders(200, resp.length);
                try (var os = exchange.getResponseBody()) { os.write(resp); }
                return;
            }
            sb.append("]}");
            byte[] response = sb.toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.getResponseHeaders().set("Cache-Control", "no-store, no-cache");
            exchange.sendResponseHeaders(200, response.length);
            try (var os = exchange.getResponseBody()) { os.write(response); }
        });

        server.createContext("/run-demo", exchange -> {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            try { exchange.getRequestBody().readAllBytes(); } catch (Exception ignored) {}
            LOG.info("Run demo requested, starting DataLocalitySimulator in background");
            var appLoader = LocalityMatchingApp.class.getClassLoader();
            Thread.ofVirtual().start(() -> {
                Thread.currentThread().setContextClassLoader(appLoader);
                try {
                    DataLocalitySimulator.main(new String[0]);
                    LOG.info("DataLocalitySimulator finished");
                } catch (Exception e) {
                    LOG.error("Demo run failed", e);
                }
            });
            byte[] response = "Demo started. Metrics update every 1s.".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(202, response.length);
            try (var os = exchange.getResponseBody()) { os.write(response); }
        });

        // Virtual thread executor — each HTTP request on its own VT
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        LOG.info("Metrics server started on http://localhost:8080/metrics");
        LOG.info("Health check: http://localhost:8080/health");
    }

    private static Properties buildStreamsConfig() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "locality-matching-v26");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                  org.apache.kafka.common.serialization.Serdes.StringSerde.class);

        // 4 StreamThreads × 3 tasks each = 12 tasks = 12 partition pairs
        // Each StreamThread processes one "shard" of the co-partitioned data
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);

        // Commit interval — how often StreamThread checkpoints offsets + RocksDB state
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // RocksDB state store directory
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/lesson26");

        // Consumer tuning for low-latency matching
        props.put(StreamsConfig.consumerPrefix("fetch.min.bytes"), 1);
        props.put(StreamsConfig.consumerPrefix("fetch.max.wait.ms"), 10);

        // Replication for internal topics (changelog of state stores)
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);

        return props;
    }

    private static record SummaryMetrics(
        String state, int activeTasks, long consumedTotal, long producedTotal,
        double processRate, double commitRate
    ) {}

    private static SummaryMetrics computeSummaryMetrics(KafkaStreams streams) {
        var state = streams.state().toString();
        int activeTasks = streams.metadataForLocalThreads().stream()
            .mapToInt(t -> t.activeTasks().size()).sum();
        long consumedTotal = 0;
        long producedTotal = 0;
        double processRate = 0;
        double commitRate = 0;
        try {
            for (var entry : streams.metrics().entrySet()) {
                var key = entry.getKey();
                var name = key.name();
                var group = key.group() != null ? key.group() : "";
                var mv = entry.getValue().metricValue();
                double value = (mv instanceof Number) ? ((Number) mv).doubleValue() : 0;
                String n = (name != null ? name : "").toLowerCase();
                String g = group.toLowerCase();
                // Kafka Streams 3.6: stream-thread-metrics has records-consumed-total, records-produced-total, process-rate, commit-rate
                boolean consumedMetric = n.contains("records-consumed-total") || n.contains("record-consumed-total")
                    || (n.contains("consumed") && (n.contains("total") || n.contains("count")));
                boolean producedMetric = n.contains("records-produced-total") || n.contains("record-produced-total")
                    || (n.contains("produced") && (n.contains("total") || n.contains("count")));
                boolean processRateMetric = n.contains("process-rate");
                boolean commitRateMetric = n.contains("commit-rate");
                if (consumedMetric) consumedTotal += (long) value;
                if (producedMetric) producedTotal += (long) value;
                if (processRateMetric) processRate += value;
                if (commitRateMetric) commitRate += value;
            }
        } catch (Exception ignored) {}
        return new SummaryMetrics(state, activeTasks, consumedTotal, producedTotal, processRate, commitRate);
    }

    private static String buildDashboardHtml(SummaryMetrics s) {
        return """
            <!DOCTYPE html>
            <html><head><meta charset="utf-8"><title>Lesson 26 Data Locality Dashboard</title>
            <link rel="stylesheet" href="/dashboard.css">
            <style>
              body { font-family: system-ui, sans-serif; max-width: 640px; margin: 2rem auto; padding: 0 1rem; }
              h1 { font-size: 1.25rem; color: #111; }
              .metrics { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin: 1.5rem 0; }
              .card { background: #f5f5f5; border-radius: 8px; padding: 1rem; }
              .card label { display: block; font-size: 0.75rem; color: #666; text-transform: uppercase; }
              .card value { font-size: 1.5rem; font-weight: 600; }
              .state { color: #0a0; }
              a { color: #06c; }
              .btn { margin: 0.5rem 0; padding: 0.5rem 1rem; background: #06c; color: #fff; border: none; border-radius: 6px; cursor: pointer; font-size: 1rem; }
              .btn:hover { background: #05a; }
              #status { font-size: 0.875rem; color: #666; margin-top: 0.5rem; }
            </style></head><body>
            <h1>Lesson 26 Data Locality Dashboard</h1>
            <p>Metrics update every 1s. <button class="btn" type="button" id="runDemoBtn">Run demo</button> <span id="status"></span></p>
            <div class="metrics">
              <div class="card"><label>State</label><value id="m-state" class="state">%s</value></div>
              <div class="card"><label>Active tasks</label><value id="m-tasks">%d</value></div>
              <div class="card"><label>Records consumed (total)</label><value id="m-consumed">%d</value></div>
              <div class="card"><label>Records produced (total)</label><value id="m-produced">%d</value></div>
              <div class="card"><label>Process rate (rec/s)</label><value id="m-process">%.2f</value></div>
              <div class="card"><label>Commit rate (commits/s)</label><value id="m-commit">%.2f</value></div>
            </div>
            <p><a href="/metrics">Raw metrics</a> &middot; <a href="/health">Health</a> &middot; <a href="/api/debug-metrics" target="_blank">Debug metrics</a></p>
            <script>
            function updateMetrics() {
              fetch('/api/metrics', { cache: 'no-store' }).then(r=>r.json()).then(d=>{
                document.getElementById('m-state').textContent = d.state;
                document.getElementById('m-tasks').textContent = d.activeTasks;
                document.getElementById('m-consumed').textContent = d.consumedTotal;
                document.getElementById('m-produced').textContent = d.producedTotal;
                document.getElementById('m-process').textContent = d.processRate.toFixed(2);
                document.getElementById('m-commit').textContent = d.commitRate.toFixed(2);
              }).catch(()=>{});
            }
            setInterval(updateMetrics, 1000);
            updateMetrics();
            document.getElementById('runDemoBtn').onclick = function(){
              var btn = this;
              btn.disabled = true;
              document.getElementById('status').textContent = 'Starting demo...';
              fetch('/run-demo', { method: 'POST', body: '' }).then(r=>r.text()).then(t=>{
                document.getElementById('status').textContent = t;
                setTimeout(function(){ btn.disabled = false; document.getElementById('status').textContent = ''; }, 12000);
              }).catch(function(){ document.getElementById('status').textContent = 'Error'; btn.disabled = false; });
            };
            </script>
            </body></html>
            """.formatted(s.state(), s.activeTasks(), s.consumedTotal(), s.producedTotal(), s.processRate(), s.commitRate());
    }
}
