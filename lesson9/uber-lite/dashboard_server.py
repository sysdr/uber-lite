#!/usr/bin/env python3
"""Lesson 9 Dashboard - Serves cluster metrics at http://localhost:8080"""
import json
import subprocess
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

PORT = 8080
KAFKA_CONTAINER = "uber-lite-kafka-1"
# Use Docker internal listener so GetOffsetShell can reach all brokers from inside container
BROKER_LIST = "kafka-1:29092,kafka-2:29092,kafka-3:29092"

# Cached metrics refreshed in background so API responds instantly
_metrics_cache = {"brokerCount": 0, "containerCount": 0, "driverLocationsTotal": 0, "error": None, "ts": 0}
_metrics_lock = threading.Lock()
_REFRESH_INTERVAL_SEC = 2

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
    out = {"brokerCount": 0, "containerCount": 0, "driverLocationsTotal": 0, "error": None}
    try:
        r = subprocess.run(
            ["docker", "ps", "--filter", "name=uber-lite-", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=5)
        names = [n for n in (r.stdout or "").strip().splitlines() if n]
        out["containerCount"] = len(names)
        out["brokerCount"] = sum(1 for n in names if "kafka" in n)
        out["driverLocationsTotal"] = _topic_offset("driver-locations")
    except Exception as e:
        out["error"] = str(e)
    out["ts"] = time.time()
    return out

def _refresh_metrics_loop():
    """Background thread: refresh metrics every REFRESH_INTERVAL_SEC so API is fast."""
    global _metrics_cache
    while True:
        try:
            data = get_metrics()
            with _metrics_lock:
                _metrics_cache = data
        except Exception as e:
            with _metrics_lock:
                _metrics_cache = {"brokerCount": 0, "containerCount": 0, "driverLocationsTotal": 0, "error": str(e), "ts": time.time()}
        time.sleep(_REFRESH_INTERVAL_SEC)

def get_cached_metrics():
    """Return current metrics (cached, updated every 2s in background)."""
    with _metrics_lock:
        return dict(_metrics_cache)

HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Lesson 9 – Cluster Dashboard</title>
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <meta http-equiv="Pragma" content="no-cache">
  <meta http-equiv="Expires" content="0">
  <style>
    /* background/gradient on html, body, #root removed so peacock green at bottom wins */
    body { font-family: system-ui,sans-serif; margin: 2rem; color: #1a202c; }
    h1 { color: #0f3460; }
    .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 1rem; margin-top: 1rem; }
    .card { background: #FFF; border: 1px solid #E5E7EB; padding: 1rem; border-radius: 8px; }
    .card .val { font-size: 1.8rem; font-weight: 700; color: #10B981; }
    .error { color: #e94560; }
    .status { font-size: 0.85rem; color: #718096; margin: 0.25rem 0 0.5rem 0; }
    html, body, #root {
      min-height: 100vh;
      background-color: #E6F4F1 !important; /* peacock green */
    }
  </style>
</head>
<body>
  <h1>Lesson 9 – Kafka Cluster Dashboard</h1>
  <p id="status" class="status">Loading…</p>
  <p id="hint"></p>
  <div id="root">Loading…</div>
  <script>
    var REFRESH_INTERVAL_MS = 2000;
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
      if ((d.driverLocationsTotal||0) === 0 && !d.error) {
        hint.innerHTML = 'Run <code>./produce-demo.sh</code> then refresh. If still 0, restart dashboard: <code>./start-dashboard.sh</code>';
      } else {
        hint.innerHTML = '';
      }
      root.innerHTML = [
        '<div class="cards">',
        '<div class="card"><h3>Containers</h3><div class="val">' + (d.containerCount||0) + '</div></div>',
        '<div class="card"><h3>Brokers</h3><div class="val">' + (d.brokerCount||0) + '</div></div>',
        '<div class="card"><h3>driver-locations</h3><div class="val">' + (d.driverLocationsTotal||0) + '</div></div>',
        '</div>'
      ].join('');
      var now = new Date();
      status.textContent = 'Last updated: ' + now.toLocaleTimeString() + ' (refresh #' + refreshCount + ' every ' + (REFRESH_INTERVAL_MS/1000) + 's)';
    }

    function load() {
      var url = '/api/metrics?t=' + Date.now() + '&r=' + Math.random();
      fetch(url, { cache: 'no-store', headers: { 'Cache-Control': 'no-cache', 'Pragma': 'no-cache' } })
        .then(function(r) {
          if (!r.ok) throw new Error('API ' + r.status);
          return r.json();
        })
        .then(function(d) {
          refreshCount++;
          render(d);
        })
        .catch(function(err) {
          var root = document.getElementById('root');
          var status = document.getElementById('status');
          if (status) status.textContent = 'Last update: failed - ' + (err.message || 'Network error');
          if (root) root.innerHTML = '<p class="error">' + (err.message || 'Network error') + ' (retrying in ' + (REFRESH_INTERVAL_MS/1000) + 's)</p>';
        });
    }

    load();
    setInterval(load, REFRESH_INTERVAL_MS);
    document.addEventListener('visibilitychange', function() {
      if (document.visibilityState === 'visible') load();
    });
  </script>
</body>
</html>"""

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # Strip query string for path matching (e.g. /dashboard?x -> /dashboard)
            path = self.path.split("?")[0].rstrip("/") or "/"
            if path == "/api/metrics":
                data = get_cached_metrics()
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
            # Avoid 404 for browser requests (favicon, etc.)
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
    # Start background thread that refreshes metrics every 2 seconds
    daemon = threading.Thread(target=_refresh_metrics_loop, daemon=True)
    daemon.start()
    # Prime the cache immediately
    try:
        with _metrics_lock:
            _metrics_cache.update(get_metrics())
    except Exception:
        pass
    try:
        # Bind to 0.0.0.0 so localhost and other interfaces (e.g. WSL) can connect
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
