#!/usr/bin/env python3
"""Lesson 16 Dashboard - Serves cluster metrics at http://localhost:8080"""
import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer, BaseHTTPRequestHandler

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PRODUCE_DEMO_SCRIPT = os.path.join(SCRIPT_DIR, "produce-demo.sh")
_producer_running = False
_producer_lock = threading.Lock()

PORT = 8080
KAFKA_CONTAINER = "uber-lite-lesson16-kafka-1"
BROKER_LIST = "kafka:29092"
_metrics_cache = {"brokerCount": 0, "containerCount": 0, "driverLocationsTotal": 0, "error": None, "ts": 0}
_metrics_lock = threading.Lock()
_REFRESH_INTERVAL_SEC = 0.2
_executor = ThreadPoolExecutor(max_workers=2)

def _topic_offset(topic):
    try:
        r = subprocess.run(
            ["docker", "exec", KAFKA_CONTAINER, "kafka-run-class", "kafka.tools.GetOffsetShell",
             "--broker-list", BROKER_LIST, "--topic", topic],
            capture_output=True, text=True, timeout=5)
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
            ["docker", "ps", "--filter", "name=uber-lite-lesson16-", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=3)
        names = [n for n in (r.stdout or "").strip().splitlines() if n]
        return len(names), sum(1 for n in names if "kafka" in n)
    except Exception:
        return 0, 0

def get_metrics():
    out = {"brokerCount": 0, "containerCount": 0, "driverLocationsTotal": 0, "error": None}
    try:
        future_ps = _executor.submit(_docker_ps)
        future_offset = _executor.submit(_topic_offset, "driver-locations")
        container_count, broker_count = future_ps.result(timeout=5)
        out["containerCount"] = container_count
        out["brokerCount"] = broker_count
        out["driverLocationsTotal"] = future_offset.result(timeout=6)
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
                _metrics_cache = {"brokerCount": 0, "containerCount": 0, "driverLocationsTotal": 0, "error": str(e), "ts": time.time()}
        time.sleep(_REFRESH_INTERVAL_SEC)

def get_cached_metrics():
    with _metrics_lock:
        return dict(_metrics_cache)

def _run_produce_demo():
    """Run produce-demo.sh in background. Called from daemon thread."""
    global _producer_running
    try:
        subprocess.run(
            ["/bin/bash", PRODUCE_DEMO_SCRIPT],
            cwd=SCRIPT_DIR,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except Exception:
        pass
    finally:
        with _producer_lock:
            _producer_running = False

def start_producer():
    """Start produce-demo.sh if not already running. Returns status dict."""
    global _producer_running
    with _producer_lock:
        if _producer_running:
            return {"status": "already_running"}
        if not os.path.isfile(PRODUCE_DEMO_SCRIPT):
            return {"status": "error", "message": "produce-demo.sh not found"}
        _producer_running = True
    t = threading.Thread(target=_run_produce_demo, daemon=True)
    t.start()
    return {"status": "started"}

def is_producer_running():
    with _producer_lock:
        return _producer_running

HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Lesson 16 – linger.ms Benchmark Dashboard</title>
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <meta http-equiv="Pragma" content="no-cache">
  <meta http-equiv="Expires" content="0">
  <style>
    * { box-sizing: border-box; }
    body { font-family: 'Segoe UI', system-ui, sans-serif; margin: 0; color: #1a202c; }
    .header { background: #0f3460; color: #fff; padding: 1rem 2rem; display: flex; align-items: center; justify-content: space-between; box-shadow: 0 2px 8px rgba(0,0,0,0.15); }
    .header h1 { margin: 0; font-size: 2rem; font-weight: 600; }
    .header .live { background: #10B981; color: #fff; padding: 0.25rem 0.6rem; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }
    .main { padding: 2rem; max-width: 1200px; margin: 0 auto; }
    .metrics-row { display: grid; grid-template-columns: 1fr 1fr 2fr; gap: 1.5rem; margin-top: 1.5rem; }
    .card { background: #FFF; border-radius: 12px; padding: 1.5rem; box-shadow: 0 2px 12px rgba(0,0,0,0.08); border: 1px solid #E5E7EB; }
    .card.primary { grid-column: span 1; }
    .card.featured { grid-column: span 1; background: linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%); border-color: #86efac; }
    .card h3 { margin: 0 0 0.5rem 0; font-size: 0.85rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; }
    .card .val { font-size: 2rem; font-weight: 700; color: #0f3460; transition: background 0.3s; }
    .card.featured .val { font-size: 2.5rem; color: #059669; }
    .card .val.updated { background: rgba(16,185,129,0.25); border-radius: 8px; padding: 0.25rem; animation: pulse 0.5s ease-out; }
    @keyframes pulse { 0% { transform: scale(1); } 50% { transform: scale(1.02); } 100% { transform: scale(1); } }
    .error { color: #e94560; }
    .status-bar { background: #f8fafc; padding: 0.75rem 2rem; font-size: 1.5rem; color: #64748b; border-bottom: 1px solid #e2e8f0; }
    .hint { padding: 1rem 2rem; font-size: 0.9rem; color: #475569; }
    html, body { min-height: 100vh; background-color: #f1f5f9 !important; }
  </style>
</head>
<body>
  <div class="header">
    <h1>Lesson 16 – linger.ms Benchmark Dashboard</h1>
    <span class="live">LIVE</span>
  </div>
  <div class="status-bar"><span id="status">Loading…</span><button id="refreshBtn" onclick="load(true)" style="margin-left:1rem;padding:0.25rem 0.6rem;cursor:pointer;border-radius:4px;border:1px solid #cbd5e1;background:#fff;">Refresh</button><button id="produceBtn" onclick="runProduce()" style="margin-left:1rem;padding:0.25rem 0.6rem;cursor:pointer;border-radius:4px;border:1px solid #10B981;background:#10B981;color:#fff;">Produce Driver Location Events</button></div>
  <div class="hint" id="hint"></div>
  <div class="main"><div id="root">Loading…</div></div>
  <script>
    var REFRESH_INTERVAL_MS = 400;
    var refreshCount = 0;
    var prevData = { containerCount: -1, brokerCount: -1, driverLocationsTotal: -1 };
    var lastChangeTime = null;
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
      if (d.error) hint.innerHTML = '';
      else if ((d.driverLocationsTotal||0) === 0) hint.innerHTML = 'Run <code>./produce-demo.sh</code> to add events. If still 0, restart: <code>./start-dashboard.sh</code>';
      else hint.innerHTML = '';
      var c = d.containerCount||0, b = d.brokerCount||0, dl = d.driverLocationsTotal||0;
      var changed = (c !== prevData.containerCount) || (b !== prevData.brokerCount) || (dl !== prevData.driverLocationsTotal);
      if (changed) lastChangeTime = new Date();
      var cCls = (c !== prevData.containerCount) ? ' val updated' : ' val';
      var bCls = (b !== prevData.brokerCount) ? ' val updated' : ' val';
      var dlCls = (dl !== prevData.driverLocationsTotal) ? ' val updated' : ' val';
      prevData = { containerCount: c, brokerCount: b, driverLocationsTotal: dl };
      root.innerHTML = [
        '<div class="metrics-row">',
        '<div class="card primary"><h3>Containers</h3><div class="' + cCls.trim() + '">' + c + '</div></div>',
        '<div class="card primary"><h3>Brokers</h3><div class="' + bCls.trim() + '">' + b + '</div></div>',
        '<div class="card featured"><h3>driver-locations (events)</h3><div class="' + dlCls.trim() + '">' + dl + '</div></div>',
        '</div>'
      ].join('');
      var now = new Date();
      var serverTs = d.ts ? new Date(d.ts * 1000).toLocaleTimeString() : 'N/A';
      var changeStr = lastChangeTime ? ' | Values changed: ' + lastChangeTime.toLocaleTimeString() : '';
      status.textContent = 'Fetched: ' + now.toLocaleTimeString() + ' | Server: ' + serverTs + changeStr + ' | #' + refreshCount;
    }
    function load(forceRefresh) {
      var url = '/api/metrics?t=' + Date.now() + '&r=' + Math.random() + (forceRefresh ? '&force=1' : '');
      fetch(url, { cache: 'no-store', headers: { 'Cache-Control': 'no-cache', 'Pragma': 'no-cache' } })
        .then(function(r) { if (!r.ok) throw new Error('API ' + r.status); return r.json(); })
        .then(function(d) { refreshCount++; render(d); })
        .catch(function(err) {
          var root = document.getElementById('root');
          var status = document.getElementById('status');
          if (status) status.textContent = 'Last update: failed - ' + (err.message || 'Network error');
          if (root) root.innerHTML = '<p class="error">' + (err.message || 'Network error') + ' (retrying in ' + (REFRESH_INTERVAL_MS/1000) + 's)</p>';
        });
    }
    function runProduce() {
      var btn = document.getElementById('produceBtn');
      var statusEl = document.getElementById('status');
      if (btn.disabled) return;
      btn.disabled = true;
      statusEl.textContent = 'Running producer…';
      fetch('/api/produce', { method: 'POST', headers: { 'Content-Type': 'application/json' } })
        .then(function(r) { return r.json(); })
        .then(function(d) {
          if (d.status === 'started') pollProducerStatus(btn, statusEl);
          else if (d.status === 'already_running') { statusEl.textContent = 'Already running'; btn.disabled = false; }
          else if (d.status === 'error') { statusEl.textContent = 'Error: ' + (d.message || 'Unknown'); btn.disabled = false; }
        })
        .catch(function() { statusEl.textContent = 'Request failed'; btn.disabled = false; });
    }
    function pollProducerStatus(btn, statusEl) {
      var check = function() {
        fetch('/api/producer-status').then(function(r) { return r.json(); }).then(function(d) {
          if (!d.running) { btn.disabled = false; statusEl.textContent = 'Producer completed'; return; }
          setTimeout(check, 1500);
        }).catch(function() { setTimeout(check, 1500); });
      };
      setTimeout(check, 2000);
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
            if path == "/api/producer-status":
                data = {"running": is_producer_running()}
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps(data).encode("utf-8"))
                return
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

    def do_POST(self):
        try:
            path = self.path.split("?")[0].rstrip("/") or "/"
            if path == "/api/produce":
                data = start_producer()
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps(data).encode("utf-8"))
                return
            self.send_response(404)
            self.end_headers()
        except Exception as e:
            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": str(e)}).encode("utf-8"))

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
