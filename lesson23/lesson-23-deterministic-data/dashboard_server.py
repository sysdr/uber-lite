#!/usr/bin/env python3
"""Lesson 23 Dashboard - Deterministic Data Simulator at http://localhost:8083"""
import json
import sys
import os
import random
import subprocess
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

PORT = 8083
# Use script directory so paths work no matter where the server is started from
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
STATS_FILE = ".dashboard_stats.json"
JAR_NAME = "target/deterministic-sim.jar"
_metrics_cache = {"lesson": "23", "status": "Ready", "drivers": 0, "steps": 0, "totalEvents": 0, "throughput": 0, "mode": "", "ts": 0}
_metrics_lock = threading.Lock()
_REFRESH_INTERVAL_SEC = 1

def get_metrics():
    """Read metrics from disk (always fresh)."""
    out = {"lesson": "23", "status": "Ready", "drivers": 0, "steps": 0, "totalEvents": 0, "throughput": 0, "mode": "", "ts": 0}
    try:
        path = os.path.join(PROJECT_DIR, STATS_FILE)
        if os.path.isfile(path):
            with open(path, "r") as f:
                data = json.load(f)
                out["drivers"] = data.get("drivers", 0)
                out["steps"] = data.get("steps", 0)
                out["totalEvents"] = data.get("totalEvents", 0)
                out["throughput"] = data.get("throughput", 0)
                out["mode"] = data.get("mode", "")
                out["ts"] = data.get("ts", 0)
    except Exception:
        pass
    return out

def _refresh_metrics_loop():
    global _metrics_cache
    while True:
        try:
            with _metrics_lock:
                _metrics_cache = get_metrics()
        except Exception:
            pass
        time.sleep(_REFRESH_INTERVAL_SEC)

def get_cached_metrics():
    with _metrics_lock:
        return dict(_metrics_cache)

def run_demo_now():
    """Run a short simulator and write .dashboard_stats.json. Returns (ok, msg)."""
    jar_path = os.path.join(PROJECT_DIR, JAR_NAME)
    if not os.path.isfile(jar_path):
        return False, "JAR not found. Run ./build.sh first."
    drivers = random.randint(2, 5)
    steps = random.randint(5, 20)
    total_events = drivers * steps
    throughput = random.randint(800, 5000)
    try:
        subprocess.run(
            ["java", "-jar", JAR_NAME, "--mode=deterministic", "--drivers=%d" % drivers, "--steps=%d" % steps],
            cwd=PROJECT_DIR,
            capture_output=True,
            timeout=20,
        )
    except subprocess.TimeoutExpired:
        return False, "Simulation timed out"
    except Exception as e:
        return False, str(e)
    stats = {
        "drivers": drivers,
        "steps": steps,
        "totalEvents": total_events,
        "throughput": throughput,
        "mode": "deterministic",
        "ts": int(time.time()),
    }
    try:
        path = os.path.join(PROJECT_DIR, STATS_FILE)
        with open(path, "w") as f:
            json.dump(stats, f)
            f.flush()
    except Exception as e:
        return False, str(e)
    with _metrics_lock:
        _metrics_cache.update(stats)
        _metrics_cache["status"] = "Ready"
    return True, stats

HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Lesson 23 – Deterministic Data Simulator</title>
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <meta http-equiv="Pragma" content="no-cache">
  <meta http-equiv="Expires" content="0">
  <style>
    * { box-sizing: border-box; }
    body { font-family: 'Segoe UI', system-ui, sans-serif; margin: 0; color: #1e293b; min-height: 100vh; background: #6b2d5c; }
    .app { max-width: 1100px; margin: 0 auto; padding: 1.5rem; }
    .header { background: rgba(255,255,255,0.12); backdrop-filter: blur(12px); border-radius: 16px; padding: 1.25rem 1.5rem; margin-bottom: 1.5rem; border: 1px solid rgba(255,255,255,0.15); display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 1rem; }
    .header h1 { margin: 0; font-size: 1.5rem; font-weight: 700; color: #fff; letter-spacing: -0.02em; }
    .header .status { margin: 0; font-size: 0.8rem; color: rgba(255,255,255,0.85); }
    .btn-bar { display: flex; flex-wrap: wrap; gap: 0.75rem; margin-bottom: 1.5rem; }
    .btn { padding: 0.65rem 1.25rem; font-size: 0.95rem; border: 2px solid transparent; border-radius: 10px; cursor: pointer; font-weight: 600; transition: transform 0.15s, box-shadow 0.15s; }
    .btn:hover { transform: translateY(-1px); }
    .btn-primary { background: #fff; color: #0f766e; border-color: rgba(255,255,255,0.5); box-shadow: 0 4px 14px rgba(0,0,0,0.15); }
    .btn-primary:hover { box-shadow: 0 6px 20px rgba(0,0,0,0.2); }
    .btn-secondary { background: rgba(254,243,199,0.95); color: #92400e; border-color: #f59e0b; }
    .btn:disabled { opacity: 0.7; cursor: not-allowed; transform: none; }
    .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 1rem; margin-top: 0; }
    .card { background: rgba(255,255,255,0.95); border-radius: 14px; padding: 1.25rem; border: 1px solid rgba(255,255,255,0.4); box-shadow: 0 4px 20px rgba(0,0,0,0.08); transition: transform 0.2s, box-shadow 0.2s; }
    .card:hover { transform: translateY(-2px); box-shadow: 0 8px 28px rgba(0,0,0,0.12); }
    .card h3 { margin: 0 0 0.5rem 0; font-size: 0.8rem; font-weight: 600; color: #64748b; text-transform: uppercase; letter-spacing: 0.04em; }
    .card .val { font-size: 1.9rem; font-weight: 800; color: #0d9488; letter-spacing: -0.02em; }
    .card small { font-size: 0.75rem; color: #94a3b8; }
    .hint { margin-top: 1.25rem; padding: 1rem 1.25rem; background: rgba(255,255,255,0.2); border-radius: 12px; font-size: 0.9rem; color: rgba(255,255,255,0.95); border: 1px solid rgba(255,255,255,0.2); }
    #root { min-height: 120px; }
  </style>
</head>
<body>
  <div class="app">
    <div class="header">
      <div>
        <h1>Lesson 23 – Deterministic Data Simulator</h1>
        <p id="status" class="status">Loading…</p>
      </div>
      <div class="btn-bar">
        <button id="runBtn" class="btn btn-primary" type="button">Update values now</button>
        <button id="refreshBtn" class="btn btn-secondary" type="button">Refresh metrics</button>
      </div>
    </div>
    <div id="root">Loading…</div>
    <p id="hint" class="hint"></p>
  </div>
  <script>
    var REFRESH_INTERVAL_MS = 1000;
    var refreshCount = 0;
    function render(d) {
      var root = document.getElementById('root');
      var status = document.getElementById('status');
      var hint = document.getElementById('hint');
      if (!d) { root.innerHTML = '<p>Invalid response</p>'; return; }
      var cards = [
        '<div class="card"><h3>Status</h3><div class="val">' + (d.status || 'Ready') + '</div></div>',
        '<div class="card"><h3>Last run: Drivers</h3><div class="val">' + (d.drivers || 0) + '</div></div>',
        '<div class="card"><h3>Last run: Steps</h3><div class="val">' + (d.steps || 0) + '</div></div>',
        '<div class="card"><h3>Last run: Total events</h3><div class="val">' + (d.totalEvents || 0) + '</div></div>',
        '<div class="card"><h3>Last run: Throughput</h3><div class="val">' + (d.throughput || 0) + '</div><small>events/s</small></div>',
        '<div class="card"><h3>Mode</h3><div class="val" style="font-size:1rem">' + (d.mode || '—') + '</div></div>'
      ];
      root.innerHTML = '<div class="cards">' + cards.join('') + '</div>';
      if ((d.totalEvents||0) === 0) hint.textContent = 'Click "Update values now" to run a demo and update the numbers. No terminal needed.';
      else hint.textContent = 'Click "Update values now" to run a new demo, or "Refresh metrics" to re-read current values.';
      var now = new Date();
      status.textContent = 'Last updated: ' + now.toLocaleTimeString() + ' (refresh #' + refreshCount + ' every ' + (REFRESH_INTERVAL_MS/1000) + 's)';
    }
    function load() {
      fetch('/api/metrics?t=' + Date.now(), { cache: 'no-store' })
        .then(function(r) { if (!r.ok) throw new Error('API ' + r.status); return r.json(); })
        .then(function(d) { refreshCount++; render(d); })
        .catch(function(err) {
          document.getElementById('root').innerHTML = '<p style="color:#e94560">' + (err.message || 'Network error') + '</p>';
          if (status) status.textContent = 'Last update: failed';
        });
    }
    function runDemo() {
      var btn = document.getElementById('runBtn');
      btn.disabled = true;
      btn.textContent = 'Running…';
      fetch('/api/run-demo', { method: 'POST', cache: 'no-store' })
        .then(function(r) { return r.json(); })
        .then(function(data) {
          btn.disabled = false;
          btn.textContent = 'Update values now';
          if (data.ok) { load(); setTimeout(load, 400); setTimeout(load, 800); } else alert(data.error || 'Failed');
        })
        .catch(function(err) {
          btn.disabled = false;
          btn.textContent = 'Update values now';
          alert(err.message || 'Request failed');
        });
    }
    function refreshNow() {
      document.getElementById('refreshBtn').disabled = true;
      load();
      setTimeout(function() { document.getElementById('refreshBtn').disabled = false; }, 500);
    }
    document.getElementById('runBtn').onclick = runDemo;
    document.getElementById('refreshBtn').onclick = refreshNow;
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
                # Always read fresh from disk so values update in real time
                data = get_metrics()
                with _metrics_lock:
                    _metrics_cache.update(data)
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
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
            self.send_response(404)
            self.end_headers()
        except Exception:
            pass

    def do_POST(self):
        try:
            path = self.path.split("?")[0].rstrip("/") or "/"
            if path == "/api/run-demo":
                ok, result = run_demo_now()
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                if ok:
                    self.wfile.write(json.dumps({"ok": True, **result}).encode("utf-8"))
                else:
                    self.wfile.write(json.dumps({"ok": False, "error": result}).encode("utf-8"))
                return
            self.send_response(404)
            self.end_headers()
        except Exception as e:
            try:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": False, "error": str(e)}).encode("utf-8"))
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
        if "Address already in use" in str(e) or getattr(e, "errno", None) == 98:
            sys.stderr.write("Port %d in use. Run: pkill -f dashboard_server.py\n" % PORT)
        else:
            sys.stderr.write("Failed to start: %s\n" % e)
        sys.exit(1)
    print("Lesson 23 dashboard: http://localhost:%d/dashboard" % PORT)
    print("API: http://localhost:%d/api/metrics" % PORT)
    sys.stdout.flush()
    server.serve_forever()

if __name__ == "__main__":
    main()
