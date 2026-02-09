#!/usr/bin/env python3
"""Lesson 18 Dashboard - H3-Aware Driver Model metrics at http://localhost:8082"""
import json
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer, BaseHTTPRequestHandler

PORT = 8082
PROJECT_FILTER = "lesson18-driver-model"
BROKER_LIST = "localhost:9092"
TOPIC = "driver-locations"
_metrics_cache = {"brokerCount": 0, "containerCount": 0, "driverLocationsTotal": 0, "partitionCount": 0,
                  "partitionSkew": 0.0, "messageRate": 0.0, "error": None, "ts": 0}
_last_total = 0
_last_ts = 0
_rate_lock = threading.Lock()
_metrics_lock = threading.Lock()
_REFRESH_INTERVAL_SEC = 1
_DEMO_PRODUCE_INTERVAL_SEC = 6
_executor = ThreadPoolExecutor(max_workers=4)

def _get_kafka_container():
    try:
        r = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=3)
        for name in (r.stdout or "").strip().splitlines():
            if "-kafka-1" in name and "kafka-ui" not in name:
                return name
        return None
    except Exception:
        return None

def _produce_demo_messages():
    """Produce messages to driver-locations so dashboard shows live updates."""
    try:
        container = _get_kafka_container()
        if not container:
            return
        ts_ms = int(time.time() * 1000)
        for i in range(15):
            msg = '{"id":"demo-%04d","h3Cell":6157323354570604543,"heading":%d,"speedMps":%.1f,"timestamp":"%s"}\n' % (
                i, (i * 24) % 360, 8.0 + (i % 5), time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
            subprocess.run(
                ["docker", "exec", "-i", container, "kafka-console-producer",
                 "--bootstrap-server", BROKER_LIST, "--topic", TOPIC],
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
        if not container:
            return 0, [], 0
        r = subprocess.run(
            ["docker", "exec", container, "kafka-run-class", "kafka.tools.GetOffsetShell",
             "--broker-list", BROKER_LIST, "--topic", topic],
            capture_output=True, text=True, timeout=6)
        total = 0
        per_part = []
        for line in (r.stdout or "").strip().splitlines():
            parts = line.strip().split(":")
            if len(parts) >= 3:
                try:
                    val = int(parts[-1])
                    total += val
                    per_part.append(val)
                except ValueError:
                    pass
        return total, per_part, len(per_part)
    except Exception:
        return 0, [], 0

def _docker_ps():
    try:
        r = subprocess.run(
            ["docker", "ps", "--filter", "name=%s" % PROJECT_FILTER, "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=3)
        names = [n for n in (r.stdout or "").strip().splitlines() if n]
        return len(names), sum(1 for n in names if "kafka" in n and "kafka-ui" not in n)
    except Exception:
        return 0, 0

def get_metrics():
    global _last_total, _last_ts
    out = {"brokerCount": 0, "containerCount": 0, "driverLocationsTotal": 0, "partitionCount": 0,
           "partitionSkew": 0.0, "messageRate": 0.0, "error": None}
    try:
        futures = [_executor.submit(_docker_ps), _executor.submit(_topic_offset, TOPIC)]
        container_count, broker_count = futures[0].result(timeout=5)
        total, per_part, part_count = futures[1].result(timeout=8)
        out["containerCount"] = container_count
        out["brokerCount"] = broker_count
        out["driverLocationsTotal"] = total
        out["partitionCount"] = part_count
        # Partition skew: max/min (target < 1.15)
        if per_part and min(per_part) > 0:
            out["partitionSkew"] = round(max(per_part) / min(per_part), 2)
        # Message rate: delta over last interval
        now = time.time()
        with _rate_lock:
            if _last_ts > 0 and _last_total >= 0:
                dt = now - _last_ts
                if dt > 0.5:
                    out["messageRate"] = round((total - _last_total) / dt, 1)
            _last_total = total
            _last_ts = now
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
                _metrics_cache = {"brokerCount": 0, "containerCount": 0, "driverLocationsTotal": 0,
                    "partitionCount": 0, "partitionSkew": 0.0, "messageRate": 0.0,
                    "error": str(e), "ts": time.time()}
        time.sleep(_REFRESH_INTERVAL_SEC)

def get_cached_metrics():
    with _metrics_lock:
        return dict(_metrics_cache)

HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Lesson 18 – H3-Aware Driver Model Dashboard</title>
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
    .card small { display: block; font-size: 0.7rem; color: #64748b; margin-top: 0.25rem; }
    .error { color: #e94560; }
    .refresh-btn { padding: 0.5rem 1rem; font-size: 0.95rem; font-weight: 600; cursor: pointer; border-radius: 6px; border: none; background: #10B981; color: #fff; transition: background 0.2s; }
    .refresh-btn:hover { background: #059669; }
    .status-bar { background: #c9a0a0; padding: 0.75rem 2rem; font-size: 0.9rem; color: #4a3d5c; border-bottom: 1px solid #d4c4e0; display: flex; align-items: center; gap: 1rem; }
    .hint { padding: 1rem 2rem; font-size: 0.9rem; color: #4a3d5c; background: #c9a0a0; }
    .main { background: #c9a0a0; }
    html, body, #root { min-height: 100vh; background-color: #c9a0a0 !important; }
  </style>
</head>
<body>
  <div class="header">
    <h1>Lesson 18 – H3-Aware Driver Model Dashboard</h1>
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
      var total = d.driverLocationsTotal || 0;
      if (total === 0 && !d.error) hint.innerHTML = 'Run <code>./demo.sh</code> to add events. Dashboard auto-produces demo messages every 6s.';
      else hint.innerHTML = '';
      var partCount = d.partitionCount||0, skew = d.partitionSkew||0, rate = d.messageRate||0;
      root.innerHTML = [
        '<div class="dashboard-grid">',
        '<div class="cluster-sidebar">',
        '<div class="card"><h3>Containers</h3><div class="val">' + (d.containerCount||0) + '</div></div>',
        '<div class="card"><h3>Brokers</h3><div class="val">' + (d.brokerCount||0) + '</div></div>',
        '<div class="card"><h3>Partitions</h3><div class="val">' + partCount + '</div></div>',
        '</div>',
        '<div class="topic-grid">',
        '<div class="card featured"><h3>driver-locations (total events)</h3><div class="val">' + total + '</div></div>',
        '<div class="card featured"><h3>Partition skew</h3><div class="val">' + skew + '</div><small>target &lt;1.15</small></div>',
        '<div class="card featured"><h3>Message rate (msg/sec)</h3><div class="val">' + rate + '</div><small>expect ~500</small></div>',
        '</div>',
        '</div>'
      ].join('');
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
