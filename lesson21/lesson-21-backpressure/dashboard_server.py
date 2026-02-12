#!/usr/bin/env python3
"""Lesson 21 Dashboard - Backpressure metrics at http://localhost:8080"""
import json
import os
import subprocess
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

PORT = 8080
BROKER_LIST = "kafka:9092"
_metrics_cache = {"brokerCount": 0, "containerCount": 0, "driverLocationsTotal": 0, "error": None, "ts": 0}
_metrics_lock = threading.Lock()
_REFRESH_INTERVAL_SEC = 2.5
_REQUEST_REFRESH_TIMEOUT = 3

def _get_kafka_container():
    try:
        r = subprocess.run(
            ["docker", "ps", "--filter", "name=lesson-21-backpressure-kafka-1", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=3)
        names = [n for n in (r.stdout or "").strip().splitlines() if n]
        return names[0] if names else "lesson-21-backpressure-kafka-1"
    except Exception:
        return "lesson-21-backpressure-kafka-1"

def _topic_offset(topic):
    try:
        container = _get_kafka_container()
        r = subprocess.run(
            ["docker", "exec", container, "kafka-run-class", "kafka.tools.GetOffsetShell",
             "--broker-list", BROKER_LIST, "--topic", topic],
            capture_output=True, text=True, timeout=_REQUEST_REFRESH_TIMEOUT)
        total = 0
        for line in (r.stdout or "").strip().splitlines():
            parts = line.strip().split(":")
            if len(parts) >= 3:
                try:
                    total += int(parts[-1])
                except ValueError:
                    pass
        return total
    except Exception:
        return 0

def get_metrics():
    out = {"brokerCount": 0, "containerCount": 0, "driverLocationsTotal": 0, "error": None}
    try:
        r = subprocess.run(
            ["docker", "ps", "--filter", "name=lesson-21-backpressure-", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=_REQUEST_REFRESH_TIMEOUT)
        names = [n for n in (r.stdout or "").strip().splitlines() if n]
        out["containerCount"] = len(names)
        out["brokerCount"] = sum(1 for n in names if "kafka" in n and "init" not in n)
        out["driverLocationsTotal"] = _topic_offset("driver-locations")
    except Exception as e:
        out["error"] = str(e)
    out["ts"] = time.time()
    return out

def _refresh_metrics_loop():
    global _metrics_cache
    while True:
        t0 = time.time()
        try:
            data = get_metrics()
            with _metrics_lock:
                _metrics_cache = data
        except Exception as e:
            with _metrics_lock:
                _metrics_cache = {"brokerCount": 0, "containerCount": 0, "driverLocationsTotal": 0, "error": str(e), "ts": time.time()}
        elapsed = time.time() - t0
        time.sleep(max(0.1, _REFRESH_INTERVAL_SEC - elapsed))

def get_cached_metrics():
    with _metrics_lock:
        return dict(_metrics_cache)

HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Lesson 21 – Backpressure Dashboard</title>
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <style>
    * { box-sizing: border-box; }
    body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; margin: 0; color: #1e293b; background: linear-gradient(160deg, #f1f5f9 0%, #e2e8f0 50%, #cbd5e1 100%); min-height: 100vh; }
    .header { background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%); color: #fff; padding: 1.25rem 2rem; display: flex; align-items: center; justify-content: space-between; box-shadow: 0 4px 14px rgba(0,0,0,0.2); }
    .header h1 { margin: 0; font-size: 1.75rem; font-weight: 700; letter-spacing: -0.02em; }
    .header .badge { background: #22c55e; color: #fff; padding: 0.35rem 0.75rem; border-radius: 9999px; font-size: 0.75rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; }
    .main { max-width: 960px; margin: 0 auto; padding: 2rem; }
    .toolbar { display: flex; flex-wrap: wrap; align-items: center; gap: 0.75rem; margin-bottom: 1.25rem; padding: 1rem; background: #fff; border-radius: 12px; box-shadow: 0 2px 12px rgba(0,0,0,0.06); border: 1px solid #e2e8f0; }
    .status { font-size: 0.875rem; color: #64748b; flex: 1; min-width: 200px; }
    .btn-group { display: flex; flex-wrap: wrap; gap: 0.5rem; }
    .refresh-btn, .restart-btn, .demo-btn { padding: 0.6rem 1.25rem; font-size: 0.9rem; font-weight: 600; cursor: pointer; border-radius: 8px; border: none; color: #fff; box-shadow: 0 2px 6px rgba(0,0,0,0.12); transition: transform 0.15s, box-shadow 0.15s; }
    .refresh-btn:hover:not(:disabled), .restart-btn:hover:not(:disabled), .demo-btn:hover:not(:disabled) { transform: translateY(-1px); box-shadow: 0 4px 12px rgba(0,0,0,0.18); }
    .refresh-btn { background: linear-gradient(180deg, #22c55e 0%, #16a34a 100%); }
    .refresh-btn:hover:not(:disabled) { background: linear-gradient(180deg, #16a34a 0%, #15803d 100%); }
    .restart-btn { background: linear-gradient(180deg, #3b82f6 0%, #2563eb 100%); }
    .restart-btn:hover:not(:disabled) { background: linear-gradient(180deg, #2563eb 0%, #1d4ed8 100%); }
    .demo-btn { background: linear-gradient(180deg, #8b5cf6 0%, #7c3aed 100%); }
    .demo-btn:hover:not(:disabled) { background: linear-gradient(180deg, #7c3aed 0%, #6d28d9 100%); }
    .refresh-btn:disabled, .restart-btn:disabled, .demo-btn:disabled { background: #94a3b8; cursor: not-allowed; transform: none; box-shadow: none; }
    .hint { margin-bottom: 1rem; padding: 0.875rem 1.25rem; background: linear-gradient(90deg, #fef3c7 0%, #fde68a 100%); border-radius: 10px; border-left: 4px solid #f59e0b; font-size: 0.9rem; color: #92400e; }
    .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 1.25rem; }
    .card { background: #fff; padding: 1.5rem; border-radius: 12px; box-shadow: 0 4px 14px rgba(0,0,0,0.06); border: 1px solid #e2e8f0; transition: box-shadow 0.2s, transform 0.2s; }
    .card:hover { box-shadow: 0 8px 24px rgba(0,0,0,0.1); transform: translateY(-2px); }
    .card h3 { margin: 0 0 0.5rem 0; font-size: 0.8rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.06em; font-weight: 600; }
    .card .val { font-size: 2rem; font-weight: 800; color: #0f172a; letter-spacing: -0.02em; }
    .card.featured { background: linear-gradient(135deg, #ecfdf5 0%, #d1fae5 100%); border-color: #6ee7b7; }
    .card.featured .val { color: #059669; }
    .error { color: #dc2626; font-weight: 600; }
    #root { min-height: 120px; }
  </style>
</head>
<body>
  <div class="header">
    <h1>Lesson 21 – Backpressure Dashboard</h1>
    <span class="badge">Live</span>
  </div>
  <div class="main">
    <div class="toolbar">
      <p id="status" class="status">Loading...</p>
      <div class="btn-group">
        <button id="refreshBtn" class="refresh-btn" onclick="refreshNow()">Refresh now</button>
        <button id="restartBtn" class="restart-btn" onclick="restartApp()">Restart dashboard</button>
        <button id="demoBtn" class="demo-btn" onclick="runDemo()">Run demo</button>
      </div>
    </div>
    <p id="hint" class="hint" style="display:none;"></p>
    <div id="root">Loading...</div>
  </div>
  <script>
    var REFRESH_INTERVAL_MS = 2500;
    var pollCount = 0;
    function render(d, fromBtn) {
      var root = document.getElementById('root');
      var hint = document.getElementById('hint');
      var status = document.getElementById('status');
      var btn = document.getElementById('refreshBtn');
      if (btn && fromBtn) { btn.disabled = false; btn.textContent = 'Refresh now'; }
      if (d && d.error) {
        hint.innerHTML = '';
        root.innerHTML = '<p class="error">' + (d.error || 'Unknown error') + '</p>';
        status.textContent = 'Error';
        return;
      }
      if (!d) { root.innerHTML = '<p class="error">Invalid response</p>'; return; }
      var total = d.driverLocationsTotal || 0;
      if (total === 0 && !d.error) { hint.innerHTML = 'Run <code>./demo.sh</code> or click Run demo to produce messages.'; hint.style.display = 'block'; }
      else { hint.innerHTML = ''; hint.style.display = 'none'; }
      root.innerHTML = [
        '<div class="cards">',
        '<div class="card"><h3>Containers</h3><div class="val">' + (d.containerCount||0) + '</div></div>',
        '<div class="card"><h3>Brokers</h3><div class="val">' + (d.brokerCount||0) + '</div></div>',
        '<div class="card featured"><h3>driver-locations</h3><div class="val">' + total + '</div></div>',
        '</div>'
      ].join('');
      pollCount++;
      var dataTime = d.ts ? new Date(d.ts * 1000).toLocaleTimeString() : 'N/A';
      status.textContent = 'Data from Kafka: ' + dataTime + ' | Poll #' + pollCount + ' every 2.5s';
    }
    function load() {
      var url = '/api/metrics?t=' + Date.now() + '&r=' + Math.random();
      fetch(url, { cache: 'no-store', headers: { 'Cache-Control': 'no-cache', 'Pragma': 'no-cache' } })
        .then(function(r) { return r.ok ? r.json() : Promise.reject(new Error(r.status)); })
        .then(function(d) { render(d, false); })
        .catch(function(e) { var r = document.getElementById('root'); if (r) r.innerHTML = '<p class="error">' + (e.message || 'Network error') + '</p>'; });
    }
    function refreshNow() {
      var btn = document.getElementById('refreshBtn');
      if (btn) { btn.disabled = true; btn.textContent = 'Refreshing...'; }
      fetch('/api/metrics?t=' + Date.now() + '&refresh=1', { cache: 'no-store', headers: { 'Cache-Control': 'no-cache', 'Pragma': 'no-cache' } })
        .then(function(r) { return r.ok ? r.json() : Promise.reject(new Error(r.status)); })
        .then(function(d) { render(d, true); })
        .catch(function(e) {
          if (btn) { btn.disabled = false; btn.textContent = 'Refresh now'; }
          var r = document.getElementById('root'); if (r) r.innerHTML = '<p class="error">' + (e.message || 'Network error') + '</p>';
        });
    }
    function restartApp() {
      var btn = document.getElementById('restartBtn');
      var status = document.getElementById('status');
      if (btn) { btn.disabled = true; btn.textContent = 'Restarting...'; }
      status.textContent = 'Restarting dashboard...';
      fetch('/api/restart', { cache: 'no-store' })
        .then(function(r) {
          if (r.ok) return r.json();
          throw new Error(r.status);
        })
        .then(function() {
          status.textContent = 'Restarting dashboard... Reloading page in a few seconds.';
          setTimeout(function() { window.location.reload(true); }, 3500);
        })
        .catch(function() {
          status.textContent = 'Restarting dashboard... If the page does not reload, refresh manually.';
          setTimeout(function() { window.location.reload(true); }, 4000);
          setTimeout(function() {
            if (btn) { btn.disabled = false; btn.textContent = 'Restart dashboard'; }
          }, 6000);
        });
    }
    function runDemo() {
      var btn = document.getElementById('demoBtn');
      var status = document.getElementById('status');
      if (btn) { btn.disabled = true; btn.textContent = 'Running demo...'; }
      status.textContent = 'Running demo (45s). Metrics will update shortly.';
      fetch('/api/run-demo', { cache: 'no-store' })
        .then(function(r) { return r.ok ? r.json() : Promise.reject(new Error(r.status)); })
        .then(function(d) {
          status.textContent = (d.message || 'Demo started.') + ' Metrics will update every 2.5s.';
          setTimeout(function() {
            if (btn) { btn.disabled = false; btn.textContent = 'Run demo'; }
          }, 50000);
        })
        .catch(function() {
          status.textContent = 'Demo may have started. Check metrics in a few seconds.';
          if (btn) { btn.disabled = false; btn.textContent = 'Run demo'; }
        });
    }
    load();
    setInterval(load, REFRESH_INTERVAL_MS);
  </script>
</body>
</html>"""

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = (self.path.split("?")[0].rstrip("/") or "/")
        try:
            if path == "/api/metrics":
                query = self.path.split("?")[1] if "?" in self.path else ""
                if "refresh=1" in query or "refresh=true" in query:
                    data = get_metrics()
                    with _metrics_lock:
                        _metrics_cache.update(data)
                else:
                    data = get_cached_metrics()
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
                self.end_headers()
                self.wfile.write(json.dumps(data).encode("utf-8"))
                return
            if path == "/api/restart":
                body = json.dumps({"ok": True, "message": "Restarting..."}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                self.wfile.flush()
                def _do_restart():
                    time.sleep(2.5)
                    try:
                        script = os.path.abspath(__file__)
                        os.chdir(os.path.dirname(script))
                        os.execv(sys.executable, [sys.executable, script])
                    except Exception:
                        pass
                threading.Thread(target=_do_restart, daemon=True).start()
                return
            if path == "/api/run-demo":
                script_dir = os.path.dirname(os.path.abspath(__file__))
                demo_sh = os.path.join(script_dir, "demo.sh")
                def _run_demo():
                    try:
                        subprocess.Popen(
                            ["bash", demo_sh],
                            cwd=script_dir,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                        )
                    except Exception:
                        pass
                threading.Thread(target=_run_demo, daemon=True).start()
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True, "message": "Demo started. Metrics will update in a few seconds."}).encode("utf-8"))
                return
            if path in ("/", "/dashboard"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(HTML.encode())
                return
            self.send_response(404)
            self.end_headers()
        except Exception:
            pass
    def log_message(self, format, *args):
        pass

def main():
    threading.Thread(target=_refresh_metrics_loop, daemon=True).start()
    try:
        with _metrics_lock:
            _metrics_cache.update(get_metrics())
    except Exception:
        pass
    try:
        server = HTTPServer(("0.0.0.0", PORT), Handler)
    except OSError as e:
        if "Address already in use" in str(e) or getattr(e, "errno", None) == 98:
            sys.stderr.write("Port %d in use. Run: pkill -f dashboard_server.py\n" % PORT)
        else:
            sys.stderr.write("Failed: %s\n" % e)
        sys.exit(1)
    print("Dashboard: http://localhost:%d/dashboard" % PORT)
    sys.stdout.flush()
    server.serve_forever()

if __name__ == "__main__":
    main()
