#!/usr/bin/env python3
"""Lesson 22 Dashboard - Spatial metrics from MetricsSimulator at http://localhost:8080"""
import json
import sys
import threading
import time
import urllib.request
from http.server import HTTPServer, BaseHTTPRequestHandler

PORT = 8080
METRICS_URL = "http://localhost:8082/api/metrics"
_metrics_cache = {"res9Count": 0, "res10Count": 0, "totalLocations": 0, "averageH3Resolution": 0.0, "error": None}
_metrics_lock = threading.Lock()
_REFRESH_INTERVAL_SEC = 2

def fetch_metrics():
    try:
        req = urllib.request.Request(METRICS_URL, headers={"Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=3) as r:
            data = json.loads(r.read().decode())
            data["error"] = None
            return data
    except Exception as e:
        return {"error": str(e)}

def _refresh_metrics_loop():
    while True:
        try:
            data = fetch_metrics()
            with _metrics_lock:
                if "error" in data and data["error"]:
                    _metrics_cache["error"] = data["error"]
                else:
                    _metrics_cache.update(data)
        except Exception:
            pass
        time.sleep(_REFRESH_INTERVAL_SEC)

def get_cached_metrics():
    with _metrics_lock:
        return dict(_metrics_cache)

HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Lesson 22 – Spatial Metrics Dashboard</title>
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <link rel="stylesheet" href="/dashboard.css">
</head>
<body>
  <header class="dashboard-header">
    <h1>Lesson 22 – Spatial Metrics Dashboard</h1>
    <p id="status" class="status">Loading…</p>
    <p id="hint"></p>
  </header>
  <div id="root">Loading…</div>
  <script>
    var REFRESH_INTERVAL_MS = 2000;
    var refreshCount = 0;
    function render(d) {
      var root = document.getElementById('root');
      var hint = document.getElementById('hint');
      var status = document.getElementById('status');
      if (d && d.error) {
        hint.innerHTML = 'Run <code>./start-simulator.sh</code> to produce metrics.';
        root.innerHTML = '<p class="error">Simulator not running: ' + (d.error || '') + '</p>';
        status.textContent = 'Last update: error';
        return;
      }
      if (!d) { root.innerHTML = '<p class="error">Invalid response</p>'; return; }
      var total = d.totalLocations || 0;
      var r9 = d.res9Count || 0, r10 = d.res10Count || 0;
      var sum = r9 + r10 || 1;
      var pct9 = Math.round(100 * r9 / sum), pct10 = Math.round(100 * r10 / sum);
      if (total === 0) hint.innerHTML = 'Run <code>./start-simulator.sh</code> for non-zero metrics.';
      else hint.innerHTML = '';
      root.innerHTML = [
        '<div class="cards">',
        '<div class="card card-total"><span class="card-icon">&#128205;</span><h3>Total Locations</h3><div class="val">' + (d.totalLocations||0).toLocaleString() + '</div></div>',
        '<div class="card card-res9"><span class="card-icon">&#11043;</span><h3>Res 9 Count</h3><div class="val">' + (d.res9Count||0).toLocaleString() + '</div></div>',
        '<div class="card card-res10"><span class="card-icon">&#11043;</span><h3>Res 10 Count</h3><div class="val">' + (d.res10Count||0).toLocaleString() + '</div></div>',
        '<div class="card card-avg"><span class="card-icon">&#128203;</span><h3>Avg H3 Resolution</h3><div class="val">' + (d.averageH3Resolution||0).toFixed(2) + '</div></div>',
        '</div>',
        '<div class="chart-section"><h4>Res 9 vs Res 10</h4><div class="bar-chart"><div class="bar bar-res9" style="width:' + pct9 + '%"></div><div class="bar bar-res10" style="width:' + pct10 + '%"></div></div><div class="bar-legend"><span><em class="dot dot-9"></em> Res 9 (' + pct9 + '%)</span><span><em class="dot dot-10"></em> Res 10 (' + pct10 + '%)</span></div></div>'
      ].join('');
      status.textContent = 'Last updated: ' + new Date().toLocaleTimeString() + ' (refresh #' + refreshCount + ')';
    }
    function load() {
      fetch('/api/metrics?t=' + Date.now(), { cache: 'no-store' })
        .then(function(r) { return r.json(); })
        .then(function(d) { refreshCount++; render(d); })
        .catch(function(err) { render({error: err.message}); });
    }
    load();
    setInterval(load, REFRESH_INTERVAL_MS);
  </script>
</body>
</html>"""

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            path = self.path.split("?")[0].rstrip("/") or "/"
            if path == "/api/metrics":
                data = get_cached_metrics()
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(json.dumps(data).encode("utf-8"))
                return
            if path in ("/", "/dashboard"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(HTML.encode())
                return
            if path == "/dashboard.css":
                try:
                    css_path = __file__.replace("dashboard_server.py", "dashboard.css")
                    with open(css_path, "rb") as f:
                        css = f.read()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/css; charset=utf-8")
                    self.send_header("Cache-Control", "no-cache")
                    self.end_headers()
                    self.wfile.write(css)
                except Exception:
                    self.send_response(404)
                return
            self.send_response(404)
            self.end_headers()
        except Exception:
            pass
    def log_message(self, format, *args):
        pass

def main():
    t = threading.Thread(target=_refresh_metrics_loop, daemon=True)
    t.start()
    try:
        with _metrics_lock:
            _metrics_cache.update(fetch_metrics())
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
    sys.stdout.flush()
    server.serve_forever()

if __name__ == "__main__":
    main()
