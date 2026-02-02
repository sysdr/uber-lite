#!/usr/bin/env python3
"""Lesson 12 Dashboard - Topic Engineering metrics at http://localhost:8081"""
import json
import subprocess
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

PORT = 8081
KAFKA_CONTAINER = "uber-lite-kafka-1"
BROKER_LIST = "kafka-1:29092,kafka-2:29092,kafka-3:29092"
_metrics_cache = {
    "brokerCount": 0, "containerCount": 0,
    "driverLocationsTotal": 0, "riderRequestsTotal": 0, "rideMatchesTotal": 0,
    "error": None, "ts": 0
}
_metrics_lock = threading.Lock()
_REFRESH_INTERVAL_SEC = 1  # real-time: refresh metrics every 1 second
_DEMO_PRODUCE_INTERVAL_SEC = 4  # produce demo messages every 4 sec so zero metrics become non-zero

def _topic_offset(topic):
    try:
        r = subprocess.run(
            ["docker", "exec", KAFKA_CONTAINER, "kafka-run-class", "kafka.tools.GetOffsetShell",
             "--broker-list", BROKER_LIST, "--topic", topic],
            capture_output=True, text=True, timeout=8)
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

def get_metrics():
    out = {
        "brokerCount": 0, "containerCount": 0,
        "driverLocationsTotal": 0, "riderRequestsTotal": 0, "rideMatchesTotal": 0,
        "error": None
    }
    try:
        r = subprocess.run(
            ["docker", "ps", "--filter", "name=uber-lite-", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=5)
        names = [n for n in (r.stdout or "").strip().splitlines() if n]
        out["containerCount"] = len(names)
        out["brokerCount"] = sum(1 for n in names if "kafka" in n)
        out["driverLocationsTotal"] = _topic_offset("driver-locations")
        out["riderRequestsTotal"] = _topic_offset("rider-requests")
        out["rideMatchesTotal"] = _topic_offset("ride-matches")
    except Exception as e:
        out["error"] = str(e)
    out["ts"] = time.time()
    return out

def _produce_demo_messages():
    """Produce a few messages to rider-requests and ride-matches so dashboard shows non-zero real-time updates."""
    try:
        r = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=3)
        if KAFKA_CONTAINER not in (r.stdout or ""):
            return
        ts_ms = int(time.time() * 1000)
        for i in range(2):
            rider_msg = '{"rider_id":"r-%d","lat":40.71,"lon":-74.00,"timestamp":%d}\n' % (ts_ms + i, ts_ms + i)
            subprocess.run(
                ["docker", "exec", "-i", KAFKA_CONTAINER, "kafka-console-producer",
                 "--bootstrap-server", BROKER_LIST, "--topic", "rider-requests"],
                input=rider_msg.encode(), capture_output=True, timeout=5)
        for i in range(2):
            match_msg = '{"match_id":"m-%d","driver_id":"d1","rider_id":"r1","timestamp":%d}\n' % (ts_ms + i, ts_ms + i)
            subprocess.run(
                ["docker", "exec", "-i", KAFKA_CONTAINER, "kafka-console-producer",
                 "--bootstrap-server", BROKER_LIST, "--topic", "ride-matches"],
                input=match_msg.encode(), capture_output=True, timeout=5)
        dl_msg = '{"driverId":"demo-%d","lat":40.71,"lon":-74.0,"timestamp":%d}\n' % (ts_ms, ts_ms)
        subprocess.run(
            ["docker", "exec", "-i", KAFKA_CONTAINER, "kafka-console-producer",
             "--bootstrap-server", BROKER_LIST, "--topic", "driver-locations"],
            input=dl_msg.encode(), capture_output=True, timeout=5)
    except Exception:
        pass

def _refresh_metrics_loop():
    global _metrics_cache
    while True:
        try:
            data = get_metrics()
            with _metrics_lock:
                _metrics_cache = dict(data)
        except Exception as e:
            with _metrics_lock:
                _metrics_cache = {
                    "brokerCount": 0, "containerCount": 0,
                    "driverLocationsTotal": 0, "riderRequestsTotal": 0, "rideMatchesTotal": 0,
                    "error": str(e), "ts": time.time()
                }
        time.sleep(_REFRESH_INTERVAL_SEC)

def _demo_producer_loop():
    """Background: produce demo messages so rider-requests and ride-matches (and driver-locations) keep updating."""
    time.sleep(2)  # let server start first
    while True:
        _produce_demo_messages()
        time.sleep(_DEMO_PRODUCE_INTERVAL_SEC)

def get_cached_metrics():
    with _metrics_lock:
        return dict(_metrics_cache)

HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Lesson 12 – Topic Engineering Dashboard</title>
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <meta http-equiv="Pragma" content="no-cache">
  <meta http-equiv="Expires" content="0">
  <style>
    html { background-color: #fff9c4 !important; min-height: 100%; }
    body { font-family: system-ui,sans-serif; margin: 2rem; color: #1a202c; background-color: #fff9c4 !important; min-height: 100vh; }
    h1 { color: #1a202c; font-weight: 700; }
    .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 1rem; margin-top: 1rem; }
    .card { background: #ffffff; border: 1px solid #bdbdbd; padding: 1rem; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .card h3 { color: #333; margin: 0 0 0.5rem 0; }
    .card .val { font-size: 1.8rem; font-weight: 700; color: #1565c0; }
    .error { color: #c62828; font-weight: 600; }
    .status { font-size: 0.85rem; color: #424242; margin: 0.25rem 0 0.5rem 0; }
    #root { background-color: #fff9c4 !important; min-height: 100%; }
    #hint, #hint code { color: #333; }
  </style>
</head>
<body style="background-color:#fff9c4 !important; min-height:100vh; color:#1a202c;">
  <h1 style="color:#1a202c !important; font-weight:700;">Lesson 12 – Topic Engineering Dashboard</h1>
  <p id="status" class="status" style="color:#424242 !important;">Loading…</p>
  <p id="hint"></p>
  <div id="root" style="background-color:#fff9c4 !important;">Loading…</div>
  <script>
    var REFRESH_INTERVAL_MS = 1000;
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
      var allZero = (d.driverLocationsTotal||0) === 0 && (d.riderRequestsTotal||0) === 0 && (d.rideMatchesTotal||0) === 0;
      if (allZero && !d.error) hint.innerHTML = 'Run <code>./run-all.sh</code> or L9 <code>./produce-demo.sh</code> then refresh.';
      else hint.innerHTML = '';
      root.innerHTML = [
        '<div class="cards">',
        '<div class="card"><h3>Containers</h3><div class="val">' + (d.containerCount||0) + '</div></div>',
        '<div class="card"><h3>Brokers</h3><div class="val">' + (d.brokerCount||0) + '</div></div>',
        '<div class="card"><h3>driver-locations</h3><div class="val">' + (d.driverLocationsTotal||0) + '</div></div>',
        '<div class="card"><h3>rider-requests</h3><div class="val">' + (d.riderRequestsTotal||0) + '</div></div>',
        '<div class="card"><h3>ride-matches</h3><div class="val">' + (d.rideMatchesTotal||0) + '</div></div>',
        '</div>'
      ].join('');
      status.textContent = 'Last updated: ' + new Date().toLocaleTimeString() + ' (refresh #' + refreshCount + ' every ' + (REFRESH_INTERVAL_MS/1000) + 's)';
      status.style.color = '#424242';
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
            sys.stderr.write("Port %d in use. Run: pkill -f lesson-12-topic-engineering/dashboard_server.py\n" % PORT)
        else:
            sys.stderr.write("Failed to start: %s\n" % e)
        sys.exit(1)
    print("Lesson 12 Dashboard: http://localhost:%d/dashboard" % PORT)
    print("API: http://localhost:%d/api/metrics" % PORT)
    sys.stdout.flush()
    server.serve_forever()

if __name__ == "__main__":
    main()
