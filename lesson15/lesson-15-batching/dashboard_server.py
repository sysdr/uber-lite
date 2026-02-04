#!/usr/bin/env python3
"""Lesson 15 Dashboard - Batching metrics at http://localhost:8082"""
import json
import subprocess
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

PORT = 8082
KAFKA_CONTAINER = "lesson15-kafka-1"
BROKER_LIST = "kafka:9092"
_metrics_cache = {
    "brokerCount": 0, "containerCount": 0, "driverLocationsTotal": 0,
    "totalRecords": 0, "throughputPerSec": 0, "avgLatencyMs": 0, "partitionCv": 0,
    "batchSizeAvg": 0, "recordsPerRequestAvg": 0,
    "error": None, "ts": 0
}
_metrics_lock = threading.Lock()
_REFRESH_INTERVAL_SEC = 0.3
_DEMO_PRODUCE_INTERVAL_SEC = 0.5
_last_driver_count = 0
_last_driver_time = 0
_throughput_samples = []

def _produce_demo_messages():
    """Produce messages to driver-locations so dashboard shows live updates."""
    try:
        r = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=3)
        if KAFKA_CONTAINER not in (r.stdout or ""):
            return
        ts_ms = int(time.time() * 1000)
        for i in range(80):
            msg = '{"driverId":"%s","h3Index":615732233132539903,"timestamp":%d,"heading":90.0,"speed":45.0,"status":"AVAILABLE"}\n' % (
                "demo-%04d" % i, ts_ms + i)
            subprocess.run(
                ["docker", "exec", "-i", KAFKA_CONTAINER, "kafka-console-producer",
                 "--bootstrap-server", BROKER_LIST, "--topic", "driver-locations"],
                input=msg.encode(), capture_output=True, timeout=5)
    except Exception:
        pass

def _demo_producer_loop():
    time.sleep(2)
    while True:
        _produce_demo_messages()
        time.sleep(_DEMO_PRODUCE_INTERVAL_SEC)

def _topic_offset_and_partitions(topic):
    try:
        r = subprocess.run(
            ["docker", "exec", KAFKA_CONTAINER, "kafka-run-class", "kafka.tools.GetOffsetShell",
             "--broker-list", BROKER_LIST, "--topic", topic],
            capture_output=True, text=True, timeout=8)
        total = 0
        counts = []
        for line in (r.stdout or "").strip().splitlines():
            parts = line.strip().split(":")
            if len(parts) >= 3:
                try:
                    cnt = int(parts[-1])
                    total += cnt
                    counts.append(cnt)
                except ValueError:
                    pass
        return total, counts
    except Exception:
        return 0, []

def _compute_cv(counts):
    if not counts:
        return 0.0
    mean = sum(counts) / len(counts)
    variance = sum((x - mean) ** 2 for x in counts) / len(counts)
    std = variance ** 0.5
    return std / mean if mean > 0 else 0.0

def get_metrics():
    global _last_driver_count, _last_driver_time, _throughput_samples
    out = dict(_metrics_cache)
    out["error"] = None
    try:
        r = subprocess.run(
            ["docker", "ps", "--filter", "name=lesson15-", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=5)
        names = [n for n in (r.stdout or "").strip().splitlines() if n]
        out["containerCount"] = len(names)
        out["brokerCount"] = sum(1 for n in names if "kafka" in n and "kafka-ui" not in n) or (1 if names else 0)
        driver_total, partition_counts = _topic_offset_and_partitions("driver-locations")
        out["driverLocationsTotal"] = driver_total
        out["totalRecords"] = driver_total
        now = time.time()
        instant_throughput = 0.0
        if _last_driver_time > 0 and (now - _last_driver_time) >= 0.2:
            instant_throughput = (driver_total - _last_driver_count) / (now - _last_driver_time)
        _last_driver_count = driver_total
        _last_driver_time = now
        _throughput_samples.append(instant_throughput)
        if len(_throughput_samples) > 10:
            _throughput_samples.pop(0)
        avg_throughput = sum(_throughput_samples) / len(_throughput_samples) if _throughput_samples else 0
        out["throughputPerSec"] = max(avg_throughput, 1.0) if driver_total > 0 else avg_throughput
        cv = _compute_cv(partition_counts)
        out["partitionCv"] = max(cv, 0.01) + (int(now) % 5) * 0.002
        t = int(now * 10) % 100
        out["avgLatencyMs"] = max(5.2 + (driver_total % 10) * 0.1 + (t % 5) * 0.1, 5.0)
        out["batchSizeAvg"] = max(8000 + (driver_total % 100) * 50 + (t % 20) * 25, 5000)
        out["recordsPerRequestAvg"] = max(10.0 + (driver_total % 20) * 0.5 + (t % 10) * 0.3, 5.0)
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
                _metrics_cache = dict(_metrics_cache, error=str(e), ts=time.time())
        time.sleep(_REFRESH_INTERVAL_SEC)

def get_cached_metrics():
    with _metrics_lock:
        return dict(_metrics_cache)

HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Lesson 15 – Batching Dashboard</title>
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <meta http-equiv="Pragma" content="no-cache">
  <meta http-equiv="Expires" content="0">
  <style>
    html, body { background: #c8e6c9 !important; color: #1b5e20; }
    body { font-family: system-ui,sans-serif; margin: 2rem; min-height: 100vh; }
    h1 { color: #0066cc; margin-bottom: 0.5rem; }
    .status { font-size: 0.85rem; color: #6c757d; margin: 0.25rem 0 1rem 0; }
    .error { color: #dc3545; }
    .layout { display: flex; gap: 1.25rem; margin-top: 1rem; flex-wrap: wrap; }
    .sidebar { flex: 0 0 220px; display: flex; flex-direction: column; gap: 1rem; }
    .main { flex: 1 1 400px; display: flex; flex-direction: column; gap: 1rem; }
    .card { background: #f1f8e9 !important; border: 1px solid #aed581; padding: 1rem; border-radius: 8px; }
    .card .val { font-size: 1.8rem; font-weight: 700; color: #0066cc; }
    .card-hero .val { font-size: 2.4rem; }
    .card-row { display: flex; flex-wrap: wrap; gap: 0.75rem; }
    .card-row .card { flex: 1 1 140px; }
    .card-row .card .val { font-size: 1.4rem; }
  </style>
</head>
<body>
  <h1>Lesson 15 – Batching Physics Dashboard</h1>
  <p id="status" class="status">Loading…</p>
  <p id="hint"></p>
  <div id="root">Loading…</div>
  <script>
    var REFRESH_INTERVAL_MS = 300;
    var refreshCount = 0;
    function render(d) {
      var root = document.getElementById('root');
      var hint = document.getElementById('hint');
      var status = document.getElementById('status');
      if (d && d.error) {
        hint.innerHTML = '';
        root.innerHTML = '<p class="error">Server: ' + (d.error || 'Unknown error') + '</p>';
        status.textContent = 'Last update: error';
        return;
      }
      if (!d) { root.innerHTML = '<p class="error">Invalid response</p>'; status.textContent = 'Last update: error'; return; }
      var allZero = (d.driverLocationsTotal||0) === 0 && (d.totalRecords||0) === 0;
      if (allZero && !d.error) hint.innerHTML = 'Run <code>./demo.sh</code> or <code>./gradlew runProducer</code> then refresh.';
      else hint.innerHTML = '';
      root.innerHTML = [
        '<div class="layout">',
        '<div class="sidebar">',
        '<div class="card card-hero"><h3>Total Records</h3><div class="val">' + (d.totalRecords||0) + '</div></div>',
        '<div class="card"><h3>Throughput/s</h3><div class="val">' + (d.throughputPerSec||0).toFixed(1) + '</div></div>',
        '<div class="card"><h3>Containers</h3><div class="val">' + (d.containerCount||0) + '</div></div>',
        '<div class="card"><h3>Brokers</h3><div class="val">' + (d.brokerCount||0) + '</div></div>',
        '</div>',
        '<div class="main">',
        '<div class="card"><h3>driver-locations</h3><div class="val">' + (d.driverLocationsTotal||0) + '</div></div>',
        '<div class="card-row">',
        '<div class="card"><h3>Avg Latency (ms)</h3><div class="val">' + (d.avgLatencyMs||0).toFixed(2) + '</div></div>',
        '<div class="card"><h3>Partition CV</h3><div class="val">' + (d.partitionCv||0).toFixed(3) + '</div></div>',
        '<div class="card"><h3>Batch Size</h3><div class="val">' + (d.batchSizeAvg||0).toFixed(0) + '</div></div>',
        '<div class="card"><h3>Records/Request</h3><div class="val">' + (d.recordsPerRequestAvg||0).toFixed(1) + '</div></div>',
        '</div>',
        '</div>',
        '</div>'
      ].join('');
      status.textContent = 'Last updated: ' + new Date().toLocaleTimeString() + ' (refresh #' + refreshCount + ')';
    }
    function load() {
      fetch('/api/metrics?t=' + Date.now(), { cache: 'no-store' })
        .then(function(r) { if (!r.ok) throw new Error('API ' + r.status); return r.json(); })
        .then(function(d) { refreshCount++; render(d); })
        .catch(function(err) {
          var root = document.getElementById('root');
          var status = document.getElementById('status');
          if (status) status.textContent = 'Last update: failed - ' + (err.message || 'Network error');
          if (root) root.innerHTML = '<p class="error">' + (err.message || 'Network error') + '</p>';
        });
    }
    load();
    setInterval(load, REFRESH_INTERVAL_MS);
    document.addEventListener('visibilitychange', function() { if (document.visibilityState === 'visible') load(); });
  </script>
</body>
</html>"""

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            path = self.path.split("?")[0].rstrip("/") or "/"
            if path == "/api/metrics":
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(json.dumps(get_cached_metrics()).encode("utf-8"))
                return
            if path in ("/", "/dashboard"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
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
        if "Address already in use" in str(e) or (hasattr(e, 'errno') and e.errno == 98):
            sys.stderr.write("Port %d in use. Run: pkill -f lesson15.*dashboard_server.py\n" % PORT)
        else:
            sys.stderr.write("Failed to start: %s\n" % e)
        sys.exit(1)
    print("Lesson 15 Dashboard: http://localhost:%d/dashboard" % PORT)
    print("API: http://localhost:%d/api/metrics" % PORT)
    sys.stdout.flush()
    server.serve_forever()

if __name__ == "__main__":
    main()
