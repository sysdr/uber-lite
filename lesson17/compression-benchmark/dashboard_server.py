#!/usr/bin/env python3
"""Lesson 17 Dashboard - Compression Benchmark metrics at http://localhost:8080"""
import json
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer, BaseHTTPRequestHandler

PORT = 8080

def _get_kafka_container():
    try:
        r = subprocess.run(
            ["docker", "ps", "--filter", "name=compression-benchmark-kafka-1", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=3)
        names = [n for n in (r.stdout or "").strip().splitlines() if n]
        return names[0] if names else "compression-benchmark-kafka-1-1"
    except Exception:
        return "compression-benchmark-kafka-1-1"

KAFKA_CONTAINER = None  # Resolved at runtime
BROKER_LIST = "kafka-1:29092,kafka-2:29093,kafka-3:29094"
TOPICS = ["location-events-none", "location-events-lz4", "location-events-snappy", "location-events-gzip"]
_metrics_cache = {"brokerCount": 0, "containerCount": 0, "locationEventsNone": 0, "locationEventsLz4": 0,
                  "locationEventsSnappy": 0, "locationEventsGzip": 0, "error": None, "ts": 0}
_metrics_lock = threading.Lock()
_REFRESH_INTERVAL_SEC = 1
_DEMO_PRODUCE_INTERVAL_SEC = 8
_executor = ThreadPoolExecutor(max_workers=6)

def _produce_demo_messages():
    """Produce messages to all location-events topics so dashboard shows live updates."""
    try:
        container = _get_kafka_container()
        r = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=3)
        if "compression-benchmark-kafka-1" not in (r.stdout or ""):
            return
        ts_ms = int(time.time() * 1000)
        for i in range(20):
            msg = '{"driverId":"demo-%04d","latitude":37.75,"longitude":-122.45,"h3Index":"89283082837ffff","timestamp":%d,"status":"available"}\n' % (i, ts_ms + i)
            for topic in TOPICS:
                subprocess.run(
                    ["docker", "exec", "-i", container, "kafka-console-producer",
                     "--bootstrap-server", BROKER_LIST, "--topic", topic],
                    input=msg.encode(), capture_output=True, timeout=5)
    except Exception:
        pass

def _demo_producer_loop():
    time.sleep(5)
    while True:
        _produce_demo_messages()
        time.sleep(_DEMO_PRODUCE_INTERVAL_SEC)

def _topic_offset(topic):
    try:
        container = _get_kafka_container()
        r = subprocess.run(
            ["docker", "exec", container, "kafka-run-class", "kafka.tools.GetOffsetShell",
             "--broker-list", BROKER_LIST, "--topic", topic],
            capture_output=True, text=True, timeout=6)
        total = 0
        for line in (r.stdout or "").strip().splitlines():
            parts = line.strip().split(":")
            if len(parts) >= 3:
                try:
                    total += int(parts[-1])
                except ValueError:
                    pass
        return total
    except subprocess.TimeoutExpired:
        return 0
    except Exception:
        return 0

def _docker_ps():
    try:
        r = subprocess.run(
            ["docker", "ps", "--filter", "name=compression-benchmark-", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=3)
        names = [n for n in (r.stdout or "").strip().splitlines() if n]
        return len(names), sum(1 for n in names if "kafka" in n)
    except Exception:
        return 0, 0

def get_metrics():
    out = {"brokerCount": 0, "containerCount": 0, "locationEventsNone": 0, "locationEventsLz4": 0,
           "locationEventsSnappy": 0, "locationEventsGzip": 0, "error": None}
    try:
        futures = [_executor.submit(_docker_ps)]
        for t in TOPICS:
            futures.append(_executor.submit(_topic_offset, t))
        container_count, broker_count = futures[0].result(timeout=5)
        out["containerCount"] = container_count
        out["brokerCount"] = broker_count
        out["locationEventsNone"] = futures[1].result(timeout=8)
        out["locationEventsLz4"] = futures[2].result(timeout=8)
        out["locationEventsSnappy"] = futures[3].result(timeout=8)
        out["locationEventsGzip"] = futures[4].result(timeout=8)
    except Exception as e:
        out["error"] = str(e)
    out["ts"] = time.time()
    return out

def _refresh_metrics_loop():
    global _metrics_cache
    while True:
        try:
            data = get_metrics()
            with _metrics_lock:
                _metrics_cache = data
        except Exception as e:
            with _metrics_lock:
                _metrics_cache = {"brokerCount": 0, "containerCount": 0, "locationEventsNone": 0,
                    "locationEventsLz4": 0, "locationEventsSnappy": 0, "locationEventsGzip": 0,
                    "error": str(e), "ts": time.time()}
        time.sleep(_REFRESH_INTERVAL_SEC)

def get_cached_metrics():
    with _metrics_lock:
        return dict(_metrics_cache)

HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Lesson 17 – Compression Benchmark Dashboard</title>
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <meta http-equiv="Pragma" content="no-cache">
  <meta http-equiv="Expires" content="0">
  <style>
    * { box-sizing: border-box; }
    body { font-family: 'Segoe UI', system-ui, sans-serif; margin: 0; color: #1a202c; }
    .header { background: #0f3460; color: #fff; padding: 1rem 2rem; display: flex; align-items: center; justify-content: space-between; box-shadow: 0 2px 8px rgba(0,0,0,0.15); }
    .header h1 { margin: 0; font-size: 2rem; font-weight: 600; }
    .header .live { background: #10B981; color: #fff; padding: 0.25rem 0.6rem; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }
    .main { padding: 2rem; max-width: 1100px; margin: 0 auto; }
    .dashboard-grid { display: grid; grid-template-columns: 200px 1fr; gap: 2rem; align-items: start; }
    .cluster-sidebar { display: flex; flex-direction: column; gap: 1rem; }
    .topic-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 1.25rem; }
    .card { background: #FFF; border-radius: 12px; padding: 1.25rem; box-shadow: 0 2px 12px rgba(0,0,0,0.08); border: 1px solid #E5E7EB; }
    .card.featured { background: linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%); border-color: #86efac; }
    .card h3 { margin: 0 0 0.5rem 0; font-size: 0.8rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; }
    .card .val { font-size: 1.8rem; font-weight: 700; color: #0f3460; }
    .card.featured .val { color: #059669; }
    .error { color: #e94560; }
    .refresh-btn { padding: 0.5rem 1rem; font-size: 0.95rem; font-weight: 600; cursor: pointer; border-radius: 6px; border: none; background: #10B981; color: #fff; transition: background 0.2s; }
    .refresh-btn:hover { background: #059669; }
    .refresh-btn:disabled { background: #94a3b8; cursor: not-allowed; }
    .status-bar { background: #e8ddf0; padding: 0.75rem 2rem; font-size: 0.9rem; color: #4a3d5c; border-bottom: 1px solid #d4c4e0; display: flex; align-items: center; gap: 1rem; }
    .hint { padding: 1rem 2rem; font-size: 0.9rem; color: #4a3d5c; background: #e8ddf0; }
    .main { background: #e8ddf0; }
    html, body, #root { min-height: 100vh; background-color: #e8ddf0 !important; }
  </style>
</head>
<body>
  <div class="header">
    <h1>Lesson 17 – Compression Benchmark Dashboard</h1>
    <span class="live">LIVE</span>
  </div>
  <div class="status-bar"><span id="status">Loading…</span><button id="refreshBtn" onclick="refreshNow()" class="refresh-btn">↻ Refresh Now</button></div>
  <div class="hint" id="hint"></div>
  <div class="main"><div id="root">Loading…</div></div>
  <script>
    var REFRESH_INTERVAL_MS = 1000;
    var refreshCount = 0;
    function render(d, fromRefresh) {
      var root = document.getElementById('root');
      var hint = document.getElementById('hint');
      var status = document.getElementById('status');
      var btn = document.getElementById('refreshBtn');
      if (btn && fromRefresh) { btn.disabled = false; btn.textContent = '↻ Refresh Now'; }
      if (d && d.error) {
        hint.innerHTML = '';
        root.innerHTML = '<p class="error">Server: ' + (d.error || 'Unknown error') + '</p>';
        status.textContent = 'Last update: error';
        return;
      }
      if (!d) { root.innerHTML = '<p class="error">Invalid response</p>'; status.textContent = 'Last update: error'; return; }
      var n0 = d.locationEventsNone||0, lz4 = d.locationEventsLz4||0, snappy = d.locationEventsSnappy||0, gzip = d.locationEventsGzip||0;
      var allZero = (n0===0 && lz4===0 && snappy===0 && gzip===0);
      if (allZero && !d.error) hint.innerHTML = 'Run <code>./demo.sh</code> to add events. If still 0, restart: <code>./start-dashboard.sh</code>';
      else hint.innerHTML = '';
      root.innerHTML = [
        '<div class="dashboard-grid">',
        '<div class="cluster-sidebar">',
        '<div class="card"><h3>Containers</h3><div class="val">' + (d.containerCount||0) + '</div></div>',
        '<div class="card"><h3>Brokers</h3><div class="val">' + (d.brokerCount||0) + '</div></div>',
        '</div>',
        '<div class="topic-grid">',
        '<div class="card featured"><h3>location-events-none</h3><div class="val">' + n0 + '</div></div>',
        '<div class="card featured"><h3>location-events-lz4</h3><div class="val">' + lz4 + '</div></div>',
        '<div class="card featured"><h3>location-events-snappy</h3><div class="val">' + snappy + '</div></div>',
        '<div class="card featured"><h3>location-events-gzip</h3><div class="val">' + gzip + '</div></div>',
        '</div>',
        '</div>'
      ].join('');
      var now = new Date();
      var serverTime = d.ts ? new Date(d.ts * 1000).toLocaleTimeString() : 'N/A';
      status.textContent = 'Auto-refresh every 1s | Data from Kafka: ' + serverTime + ' | Refresh #' + refreshCount;
    }
    function load(forceRefresh) {
      var url = '/api/metrics?t=' + Date.now() + '&r=' + Math.random() + (forceRefresh ? '&force=1' : '');
      fetch(url, { cache: 'no-store', headers: { 'Cache-Control': 'no-cache', 'Pragma': 'no-cache' } })
        .then(function(r) { if (!r.ok) throw new Error('API ' + r.status); return r.json(); })
        .then(function(d) { refreshCount++; render(d, false); })
        .catch(function(err) {
          var root = document.getElementById('root');
          var status = document.getElementById('status');
          var btn = document.getElementById('refreshBtn');
          if (btn) { btn.disabled = false; btn.textContent = '↻ Refresh Now'; }
          if (status) status.textContent = 'Last update: failed - ' + (err.message || 'Network error');
          if (root) root.innerHTML = '<p class="error">' + (err.message || 'Network error') + '</p>';
        });
    }
    function refreshNow() {
      var btn = document.getElementById('refreshBtn');
      var status = document.getElementById('status');
      if (btn) { btn.disabled = true; btn.textContent = 'Refreshing…'; }
      if (status) status.textContent = 'Fetching fresh data from Kafka…';
      var url = '/api/metrics?t=' + Date.now() + '&r=' + Math.random() + '&force=1';
      fetch(url, { cache: 'no-store', headers: { 'Cache-Control': 'no-cache', 'Pragma': 'no-cache' } })
        .then(function(r) { if (!r.ok) throw new Error('API ' + r.status); return r.json(); })
        .then(function(d) { refreshCount++; render(d, true); })
        .catch(function(err) {
          var status = document.getElementById('status');
          var btn = document.getElementById('refreshBtn');
          if (btn) { btn.disabled = false; btn.textContent = '↻ Refresh Now'; }
          if (status) status.textContent = 'Refresh failed: ' + (err.message || 'Network error');
        });
    }
    load();
    setInterval(load, REFRESH_INTERVAL_MS);
    document.addEventListener('visibilitychange', function() { if (document.visibilityState === 'visible') load(false); });
  </script>
</body>
</html>"""

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            path = self.path.split("?")[0].rstrip("/") or "/"
            if path == "/api/metrics":
                query = self.path.split("?")[1] if "?" in self.path else ""
                force = "force=1" in query or "force=true" in query
                data = get_metrics() if force else get_cached_metrics()
                with _metrics_lock:
                    _metrics_cache.update(data)
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
                self.end_headers()
                self.wfile.write(json.dumps(data).encode("utf-8"))
                return
            if path in ("/", "/dashboard"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
                self.end_headers()
                self.wfile.write(HTML.encode())
                return
            if path in ("/favicon.ico", "/favicon.png"):
                self.send_response(204)
                self.end_headers()
                return
            self.send_response(404)
            self.end_headers()
        except Exception:
            pass
    def log_message(self, format, *args):
        pass

def main():
    daemon = threading.Thread(target=_refresh_metrics_loop, daemon=True)
    daemon.start()
    demo_thread = threading.Thread(target=_demo_producer_loop, daemon=True)
    demo_thread.start()
    try:
        with _metrics_lock:
            _metrics_cache.update(get_metrics())
    except Exception:
        pass
    try:
        server = HTTPServer(("0.0.0.0", PORT), Handler)
    except OSError as e:
        if "Address already in use" in str(e) or e.errno == 98:
            sys.stderr.write("Port %d in use. Run: pkill -f dashboard_server.py\n" % PORT)
        else:
            sys.stderr.write("Failed to start: %s\n" % e)
        sys.exit(1)
    print("Dashboard ready: http://localhost:%d/dashboard" % PORT)
    print("API: http://localhost:%d/api/metrics" % PORT)
    sys.stdout.flush()
    server.serve_forever()

if __name__ == "__main__":
    main()
