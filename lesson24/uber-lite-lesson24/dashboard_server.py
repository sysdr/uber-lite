#!/usr/bin/env python3
"""Lesson 24 Dashboard - Shows stress test metrics from :9090 and cluster info."""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

PORT = 8080
STRESS_METRICS_URL = "http://localhost:9090/metrics"
KAFKA_PREFIX = "lesson24-"
_REFRESH_INTERVAL_SEC = 1
_metrics_cache = {
    "sendRatePerSec": 0, "heapUsedPercent": 0, "avgLatencyMs": 0,
    "totalSent": 0, "totalErrors": 0, "uptimeSeconds": 0,
    "containerCount": 0, "brokerCount": 0,
    "stressAppReachable": False, "error": None, "ts": 0
}
_metrics_lock = threading.Lock()

def _fetch_stress_metrics():
    try:
        req = urllib.request.Request(STRESS_METRICS_URL)
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        return {"error": str(e)}

def _docker_containers():
    try:
        r = subprocess.run(
            ["docker", "ps", "--filter", "name=" + KAFKA_PREFIX, "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=5)
        names = [n for n in (r.stdout or "").strip().splitlines() if n]
        return len(names), sum(1 for n in names if "kafka" in n)
    except Exception:
        return 0, 0

def get_metrics():
    out = dict(_metrics_cache)
    out["error"] = None
    out["stressAppReachable"] = False
    stress = _fetch_stress_metrics()
    if "error" in stress:
        # Stress app not running is normal — don't set out["error"], just zero metrics
        out["sendRatePerSec"] = out["heapUsedPercent"] = out["avgLatencyMs"] = 0
        out["totalSent"] = out["totalErrors"] = out["uptimeSeconds"] = 0
    else:
        out["stressAppReachable"] = True
        out["sendRatePerSec"] = stress.get("sendRatePerSec", 0)
        out["heapUsedPercent"] = stress.get("heapUsedPercent", 0)
        out["avgLatencyMs"] = stress.get("avgLatencyMs", 0)
        out["totalSent"] = stress.get("totalSent", 0)
        out["totalErrors"] = stress.get("totalErrors", 0)
        out["uptimeSeconds"] = stress.get("uptimeSeconds", 0)
    try:
        cnt, bro = _docker_containers()
        out["containerCount"] = cnt
        out["brokerCount"] = bro
    except Exception as e:
        out["error"] = "Docker: " + str(e)
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
                _metrics_cache["error"] = str(e)
        time.sleep(_REFRESH_INTERVAL_SEC)

def get_cached_metrics():
    with _metrics_lock:
        return dict(_metrics_cache)

def _run_demo_background():
    """Start ./demo.sh in background (same dir as this script). Returns (ok, message)."""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        demo_sh = os.path.join(script_dir, "demo.sh")
        jar_path = os.path.join(script_dir, "target", "stress-test-1.0.0.jar")
        if not os.path.isfile(demo_sh):
            return False, "demo.sh not found"
        if not os.path.isfile(jar_path):
            return False, "Build the project first: run ./start.sh or ./build.sh in the project directory."
        try:
            r = subprocess.run(
                ["docker", "ps", "--format", "{{.Names}}"],
                capture_output=True, text=True, timeout=5)
            if "lesson24-kafka-1" not in (r.stdout or ""):
                return False, "Start the cluster first: run ./start.sh in the project directory."
        except Exception:
            return False, "Docker not available. Run ./start.sh first."
        subprocess.Popen(
            ["bash", demo_sh],
            cwd=script_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return True, "Demo started. Metrics usually appear within 10–15 seconds."
    except Exception as e:
        return False, str(e)

HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Lesson 24 – Stress Test Dashboard</title>
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <meta http-equiv="Pragma" content="no-cache">
  <meta http-equiv="Expires" content="0">
  <style>
    * { box-sizing: border-box; }
    html, body { min-height: 100vh; margin: 0; background: linear-gradient(145deg, #b8860b 0%, #D4AF37 35%, #F4C430 70%, #FFD700 100%); color: #1a202c; font-family: 'Segoe UI', system-ui, sans-serif; }
    .header { background: rgba(0,0,0,0.25); padding: 1.25rem 2rem; margin: 0 0 1.5rem 0; box-shadow: 0 2px 8px rgba(0,0,0,0.2); }
    .header h1 { margin: 0; color: #fff; font-size: 1.75rem; font-weight: 700; letter-spacing: -0.02em; }
    .header .sub { color: rgba(255,255,255,0.85); font-size: 0.9rem; margin-top: 0.25rem; }
    .main { max-width: 1200px; margin: 0 auto; padding: 0 2rem 2rem; }
    .toolbar-panel { background: #fff; border-radius: 12px; padding: 1.25rem 1.5rem; margin-bottom: 1.25rem; box-shadow: 0 4px 14px rgba(0,0,0,0.08); display: flex; align-items: center; gap: 1.25rem; flex-wrap: wrap; }
    .toolbar-panel .status { margin: 0; font-size: 0.875rem; color: #64748b; flex: 1; min-width: 200px; }
    .btn-wrap { display: flex; gap: 0.75rem; flex-wrap: wrap; }
    .btn { border: none; padding: 0.65rem 1.35rem; border-radius: 10px; cursor: pointer; font-weight: 600; font-size: 0.95rem; transition: transform 0.15s, box-shadow 0.15s; }
    .btn:hover { transform: translateY(-1px); }
    .btn:active { transform: translateY(0); }
    .btn-refresh { background: linear-gradient(180deg, #f1f5f9 0%, #e2e8f0 100%); color: #334155; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
    .btn-refresh:hover { box-shadow: 0 4px 12px rgba(0,0,0,0.12); }
    .btn-demo { background: linear-gradient(180deg, #10B981 0%, #059669 100%); color: #fff; box-shadow: 0 2px 8px rgba(16,185,129,0.4); }
    .btn-demo:hover { box-shadow: 0 4px 14px rgba(16,185,129,0.5); }
    #demoMsg { margin: 0 0 0.5rem 0; padding: 0.5rem 0; font-size: 0.9rem; min-height: 1.5rem; }
    #hint { margin: 0 0 1rem 0; font-size: 0.9rem; color: rgba(255,255,255,0.9); line-height: 1.4; }
    .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 1.25rem; margin-top: 0.5rem; }
    .card { background: #fff; border-radius: 12px; padding: 1.35rem; box-shadow: 0 4px 14px rgba(0,0,0,0.08); transition: transform 0.2s, box-shadow 0.2s; border: 1px solid rgba(255,255,255,0.5); }
    .card:hover { transform: translateY(-2px); box-shadow: 0 8px 24px rgba(0,0,0,0.12); }
    .card h3 { margin: 0 0 0.5rem 0; font-size: 0.8rem; font-weight: 600; color: #64748b; text-transform: uppercase; letter-spacing: 0.04em; }
    .card .val { font-size: 2rem; font-weight: 800; color: #059669; letter-spacing: -0.02em; }
    .error { color: #dc2626; font-weight: 500; }
    #root { min-height: 200px; }
  </style>
</head>
<body>
  <header class="header">
    <h1>Lesson 24 – Stress Test Dashboard</h1>
    <p class="sub">Real-time metrics from the stress test application</p>
  </header>
  <div class="main">
    <div class="toolbar-panel">
      <p id="status" class="status">Loading…</p>
      <div class="btn-wrap">
        <button type="button" class="btn btn-refresh" id="btnRefresh">Refresh now</button>
        <button type="button" class="btn btn-demo" id="btnRunDemo">Run demo</button>
      </div>
    </div>
    <p id="demoMsg"></p>
    <p id="hint"></p>
    <div id="root">Loading…</div>
  </div>
  <script>
    var REFRESH_INTERVAL_MS = 1000;
    var refreshCount = 0;
    function render(d) {
      var root = document.getElementById('root');
      var hint = document.getElementById('hint');
      var status = document.getElementById('status');
      if (!d) { root.innerHTML = '<p class="error">Invalid response</p>'; status.textContent = 'Last update: error'; return; }
      if (d.error) {
        root.innerHTML = '<p class="error">' + (d.error || 'Unknown error') + '</p>';
        status.textContent = 'Last update: error';
        hint.innerHTML = '';
        return;
      }
      root.innerHTML = [
        '<div class="cards">',
        '<div class="card"><h3>Containers</h3><div class="val">' + (d.containerCount||0) + '</div></div>',
        '<div class="card"><h3>Brokers</h3><div class="val">' + (d.brokerCount||0) + '</div></div>',
        '<div class="card"><h3>Send rate (/s)</h3><div class="val">' + (d.sendRatePerSec!=null ? Number(d.sendRatePerSec).toLocaleString(undefined,{maximumFractionDigits:0}) : '0') + '</div></div>',
        '<div class="card"><h3>Heap %</h3><div class="val">' + (d.heapUsedPercent!=null ? Number(d.heapUsedPercent).toFixed(1) : '0') + '</div></div>',
        '<div class="card"><h3>Avg latency (ms)</h3><div class="val">' + (d.avgLatencyMs!=null ? Number(d.avgLatencyMs).toFixed(2) : '0') + '</div></div>',
        '<div class="card"><h3>Total sent</h3><div class="val">' + (d.totalSent!=null ? Number(d.totalSent).toLocaleString() : '0') + '</div></div>',
        '<div class="card"><h3>Total errors</h3><div class="val">' + (d.totalErrors!=null ? d.totalErrors : '0') + '</div></div>',
        '<div class="card"><h3>Uptime (s)</h3><div class="val">' + (d.uptimeSeconds!=null ? d.uptimeSeconds : '0') + '</div></div>',
        '</div>'
      ].join('');
      if (d.stressAppReachable) {
        var now = new Date();
        status.textContent = 'Last updated: ' + now.toLocaleTimeString() + ' (refresh #' + refreshCount + ' every ' + (REFRESH_INTERVAL_MS/1000) + 's)';
        if ((d.totalSent||0) === 0) hint.innerHTML = 'Stress app is running. Run <code>./demo.sh</code> to produce load.';
        else hint.innerHTML = '';
      } else {
        status.textContent = 'Stress app not running. Click "Run demo" to see live metrics (refreshing every ' + (REFRESH_INTERVAL_MS/1000) + 's)';
        hint.innerHTML = 'Click the <strong>Run demo</strong> button above to start the stress test. Cluster metrics (Containers, Brokers) are from Docker.';
      }
    }
    function load(forceRefresh) {
      var url = '/api/metrics?t=' + Date.now();
      if (forceRefresh) url += '&refresh=1';
      fetch(url, { cache: 'no-store', headers: { 'Cache-Control': 'no-cache', 'Pragma': 'no-cache' } })
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
    setInterval(function() { load(false); }, REFRESH_INTERVAL_MS);
    document.getElementById('btnRefresh').onclick = function() { load(true); };
    document.getElementById('btnRunDemo').onclick = function() {
      var msgEl = document.getElementById('demoMsg');
      msgEl.textContent = 'Starting demo…';
      msgEl.style.color = '#718096';
      fetch('/api/run-demo', { method: 'POST', cache: 'no-store' })
        .then(function(r) { return r.json(); })
        .then(function(d) {
          msgEl.textContent = d.ok ? d.message : ('Error: ' + (d.message || 'Unknown'));
          msgEl.style.color = d.ok ? '#10B981' : '#e94560';
          if (d.ok) {
            var count = 0;
            var maxPolls = 25;
            function poll() {
              load(true);
              count++;
              if (count < maxPolls) setTimeout(poll, 1500);
            }
            setTimeout(poll, 2000);
          }
        })
        .catch(function(err) {
          msgEl.textContent = 'Error: ' + (err.message || 'Network error');
          msgEl.style.color = '#e94560';
        });
    };
    document.addEventListener('visibilitychange', function() { if (document.visibilityState === 'visible') load(true); });
  </script>
</body>
</html>"""

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            path = self.path.split("?")[0].rstrip("/") or "/"
            if path == "/api/metrics":
                qs = parse_qs(urlparse(self.path).query)
                if qs.get("refresh", [""])[0] == "1":
                    data = get_metrics()
                    with _metrics_lock:
                        _metrics_cache.update(data)
                else:
                    data = get_cached_metrics()
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.end_headers()
                self.wfile.write(json.dumps(data).encode("utf-8"))
                return
            if path in ("/", "/dashboard"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.send_header("Pragma", "no-cache")
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

    def do_POST(self):
        try:
            path = self.path.split("?")[0].rstrip("/") or "/"
            if path == "/api/run-demo":
                ok, message = _run_demo_background()
                body = json.dumps({"ok": ok, "message": message}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", len(body))
                self.end_headers()
                self.wfile.write(body)
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
