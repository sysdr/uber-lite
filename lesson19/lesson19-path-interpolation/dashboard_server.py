#!/usr/bin/env python3
"""Lesson 19 Dashboard - Serves cluster metrics at http://localhost:8080"""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEMO_SCRIPT = os.path.join(SCRIPT_DIR, "demo.sh")

PORT = 8080
KAFKA_CONTAINER = "lesson19-kafka-1"
BROKER_LIST = "localhost:9092"
_metrics_cache = {"brokerCount": 0, "containerCount": 0, "driverLocationsTotal": 0, "error": None, "ts": 0}
_metrics_lock = threading.Lock()
_REFRESH_INTERVAL_SEC = 3  # background thread updates cache every 3s

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
                    # Handle format "topic:partition:offset" or "topic:partition:offset,offset2"
                    raw = parts[-1].split(",")[0].strip()
                    total += int(raw)
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
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=5)
        names = [n for n in (r.stdout or "").strip().splitlines() if n and ("lesson19" in n or "kafka" in n.lower() or "zookeeper" in n.lower())]
        out["containerCount"] = len(names)
        out["brokerCount"] = sum(1 for n in names if "kafka" in n.lower())
        out["driverLocationsTotal"] = _topic_offset("driver-locations")
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

def run_demo():
    """Run ./demo.sh in the project directory (non-blocking)."""
    if not os.path.isfile(DEMO_SCRIPT):
        return False, "demo.sh not found"
    try:
        subprocess.Popen(
            ["bash", DEMO_SCRIPT],
            cwd=SCRIPT_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return True, "Demo started (100 location updates will be produced)"
    except Exception as e:
        return False, str(e)

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Lesson 19 – Path Interpolation Dashboard</title>
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <meta http-equiv="Pragma" content="no-cache">
  <meta http-equiv="Expires" content="0">
  <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-dark: #0f172a;
      --bg-card: #1e293b;
      --accent: #22d3ee;
      --accent-dim: #0891b2;
      --success: #34d399;
      --text: #f1f5f9;
      --text-muted: #94a3b8;
      --border: #334155;
    }
    * { box-sizing: border-box; }
    html, body { margin: 0; padding: 0; min-height: 100vh; }
    body {
      font-family: 'DM Sans', system-ui, sans-serif;
      background: #e0f2fe;
      color: var(--text);
      padding: 1.5rem;
      display: flex;
      flex-direction: column;
    }
    .header {
      margin-bottom: 1rem;
    }
    .header h1 {
      font-size: 1.6rem;
      font-weight: 700;
      margin: 0;
      background: linear-gradient(90deg, var(--accent), var(--success));
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
    }
    .hint {
      font-size: 0.85rem;
      color: var(--text-muted);
      margin: 0 0 1rem 0;
    }
    .hint code {
      background: var(--bg-card);
      padding: 0.2em 0.5em;
      border-radius: 6px;
      border: 1px solid var(--border);
    }
    .main { flex: 1; }
    .cards {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
      gap: 1.25rem;
    }
    .card {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 1.5rem;
      transition: transform 0.2s ease, box-shadow 0.2s ease;
      box-shadow: 0 4px 24px rgba(0,0,0,0.25);
    }
    .card:hover {
      transform: translateY(-4px);
      box-shadow: 0 12px 32px rgba(0,0,0,0.35);
    }
    .card-icon { font-size: 2rem; margin-bottom: 0.5rem; opacity: 0.9; }
    .card h3 {
      margin: 0 0 0.4rem 0;
      font-size: 0.75rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--text-muted);
    }
    .card .val {
      font-size: 2.2rem;
      font-weight: 700;
      color: var(--success);
      letter-spacing: -0.02em;
      transition: background-color 0.4s ease, color 0.2s ease;
    }
    .card .val.updated {
      background-color: rgba(34, 211, 238, 0.2);
      border-radius: 6px;
      padding: 0 4px;
    }
    .card.driver .val { color: var(--accent); }
    .error-msg {
      background: rgba(239, 68, 68, 0.15);
      border: 1px solid rgba(239, 68, 68, 0.5);
      color: #fca5a5;
      padding: 1rem 1.25rem;
      border-radius: 12px;
      margin: 1rem 0;
    }
    .footer {
      margin-top: 2rem;
      padding-top: 1rem;
      border-top: 1px solid var(--border);
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 1rem;
    }
    .footer .status-line {
      font-size: 0.8rem;
      color: var(--text-muted);
    }
    .footer .status-line strong { color: var(--accent); }
    .btn-refresh {
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      padding: 0.65rem 1.3rem;
      font-family: inherit;
      font-size: 0.9rem;
      font-weight: 600;
      color: var(--bg-dark);
      background: linear-gradient(135deg, var(--accent), var(--accent-dim));
      border: none;
      border-radius: 10px;
      cursor: pointer;
      box-shadow: 0 4px 14px rgba(34, 211, 238, 0.35);
      transition: transform 0.15s ease, box-shadow 0.15s ease;
    }
    .btn-refresh:hover {
      transform: translateY(-2px);
      box-shadow: 0 6px 20px rgba(34, 211, 238, 0.45);
    }
    .btn-refresh:active { transform: translateY(0); }
    .btn-refresh.loading { pointer-events: none; opacity: 0.8; }
    .btn-refresh .btn-label { min-width: 4.5em; text-align: center; }
    .footer-btns { display: flex; gap: 0.75rem; align-items: center; }
    .btn-demo {
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      padding: 0.65rem 1.3rem;
      font-family: inherit;
      font-size: 0.9rem;
      font-weight: 600;
      color: var(--bg-dark);
      background: linear-gradient(135deg, var(--success), #059669);
      border: none;
      border-radius: 10px;
      cursor: pointer;
      box-shadow: 0 4px 14px rgba(52, 211, 153, 0.35);
      transition: transform 0.15s ease, box-shadow 0.15s ease;
    }
    .btn-demo:hover {
      transform: translateY(-2px);
      box-shadow: 0 6px 20px rgba(52, 211, 153, 0.45);
    }
    .btn-demo:active { transform: translateY(0); }
    .btn-demo.loading { pointer-events: none; opacity: 0.8; }
    .btn-demo .btn-label { min-width: 4em; text-align: center; }
    .demo-msg { font-size: 0.85rem; color: var(--text-muted); margin: 0.5rem 0 0 0; min-height: 1.4em; }
    .demo-msg.ok { color: var(--success); }
    .demo-msg.err { color: #fca5a5; }
    #root:empty::after { content: 'Loading…'; color: var(--text-muted); }
  </style>
</head>
<body>
  <div class="header">
    <h1>Lesson 19 – Path Interpolation</h1>
  </div>
  <p class="hint" id="hint"></p>
  <div class="main">
    <div id="root">Loading…</div>
  </div>
  <div class="footer">
    <span class="status-line" id="status">Loading…</span>
    <div class="footer-btns">
      <button type="button" class="btn-demo" id="btnDemo" aria-label="Run demo">
        <span aria-hidden="true">▶</span>
        <span class="btn-label">Run demo</span>
      </button>
      <button type="button" class="btn-refresh" id="btnRefresh" aria-label="Refresh metrics">
        <span aria-hidden="true">↻</span>
        <span class="btn-label">Refresh now</span>
      </button>
    </div>
  </div>
  <p class="demo-msg" id="demoMsg"></p>
  <script>
    var REFRESH_INTERVAL_MS = 3000;
    var refreshCount = 0;

    function render(d) {
      var root = document.getElementById('root');
      var hint = document.getElementById('hint');
      var status = document.getElementById('status');
      var btn = document.getElementById('btnRefresh');
      var btnLabel = btn ? btn.querySelector('.btn-label') : null;

      if (d && d.error) {
        hint.innerHTML = '';
        root.innerHTML = '<div class="error-msg">Server: ' + (d.error || 'Unknown error') + '</div>';
        if (status) status.innerHTML = 'Last update: <strong>error</strong>';
        if (btnLabel) btnLabel.textContent = 'Refresh now';
        if (btn) btn.classList.remove('loading');
        return;
      }
      if (!d) {
        root.innerHTML = '<div class="error-msg">Invalid response</div>';
        if (status) status.innerHTML = 'Last update: <strong>error</strong>';
        if (btnLabel) btnLabel.textContent = 'Refresh now';
        if (btn) btn.classList.remove('loading');
        return;
      }
      if ((d.driverLocationsTotal || 0) === 0 && !d.error) {
        hint.innerHTML = 'Run <code>./demo.sh</code> to produce driver-locations, then click Refresh now below.';
      } else {
        hint.innerHTML = '';
      }

      var containers = d.containerCount || 0;
      var brokers = d.brokerCount || 0;
      var driverTotal = d.driverLocationsTotal || 0;

      root.innerHTML = [
        '<div class="cards">',
        '<div class="card"><div class="card-icon">📦</div><h3>Containers</h3><div class="val" data-val="' + containers + '">' + containers + '</div></div>',
        '<div class="card"><div class="card-icon">🔄</div><h3>Brokers</h3><div class="val" data-val="' + brokers + '">' + brokers + '</div></div>',
        '<div class="card driver"><div class="card-icon">📍</div><h3>driver-locations</h3><div class="val" data-val="' + driverTotal + '">' + driverTotal + '</div></div>',
        '</div>'
      ].join('');
      setTimeout(function() {
        var vals = root.querySelectorAll('.val');
        for (var i = 0; i < vals.length; i++) vals[i].classList.add('updated');
        setTimeout(function() {
          for (var i = 0; i < vals.length; i++) vals[i].classList.remove('updated');
        }, 500);
      }, 10);

      var now = new Date();
      if (status) status.innerHTML = 'Last updated: <strong>' + now.toLocaleTimeString() + '</strong> (every ' + (REFRESH_INTERVAL_MS/1000) + 's)';
      if (btnLabel) btnLabel.textContent = 'Refresh now';
      if (btn) btn.classList.remove('loading');
    }

    function load(forceRefresh) {
      var url = '/api/metrics?t=' + Date.now() + '&r=' + Math.random();
      if (forceRefresh) url += '&refresh=1';
      var btn = document.getElementById('btnRefresh');
      var btnLabel = btn ? btn.querySelector('.btn-label') : null;
      if (btn && forceRefresh) {
        btn.classList.add('loading');
        if (btnLabel) btnLabel.textContent = 'Updating…';
      }
      fetch(url, { method: 'GET', cache: 'no-store', headers: { 'Cache-Control': 'no-cache', 'Pragma': 'no-cache' } })
        .then(function(r) { if (!r.ok) throw new Error('API ' + r.status); return r.json(); })
        .then(function(d) { refreshCount++; render(d); })
        .catch(function(err) {
          var root = document.getElementById('root');
          var status = document.getElementById('status');
          if (status) status.innerHTML = 'Last update: <strong>failed</strong> – ' + (err.message || 'Network error');
          if (root) root.innerHTML = '<div class="error-msg">' + (err.message || 'Network error') + '</div>';
          if (btnLabel) btnLabel.textContent = 'Refresh now';
          if (btn) btn.classList.remove('loading');
        });
    }

    var btnRef = document.getElementById('btnRefresh');
    if (btnRef) {
      btnRef.addEventListener('click', function(e) {
        e.preventDefault();
        load(true);
      });
    }
    var btnDemo = document.getElementById('btnDemo');
    var demoMsg = document.getElementById('demoMsg');
    if (btnDemo) {
      btnDemo.addEventListener('click', function(e) {
        e.preventDefault();
        var label = btnDemo.querySelector('.btn-label');
        if (label) label.textContent = 'Starting…';
        btnDemo.classList.add('loading');
        if (demoMsg) { demoMsg.textContent = ''; demoMsg.className = 'demo-msg'; }
        fetch('/api/run-demo?t=' + Date.now(), { method: 'GET', cache: 'no-store' })
          .then(function(r) { return r.json(); })
          .then(function(d) {
            if (demoMsg) {
              demoMsg.textContent = d.message || (d.ok ? 'Demo started.' : 'Failed');
              demoMsg.className = 'demo-msg ' + (d.ok ? 'ok' : 'err');
            }
            if (label) label.textContent = 'Run demo';
            btnDemo.classList.remove('loading');
            if (d.ok) setTimeout(function() { load(true); }, 500);
          })
          .catch(function(err) {
            if (demoMsg) { demoMsg.textContent = err.message || 'Request failed'; demoMsg.className = 'demo-msg err'; }
            if (label) label.textContent = 'Run demo';
            btnDemo.classList.remove('loading');
          });
      });
    }
    load(false);
    setInterval(function() { load(false); }, REFRESH_INTERVAL_MS);
    document.addEventListener('visibilitychange', function() { if (document.visibilityState === 'visible') load(false); });
  </script>
</body>
</html>"""

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            parsed = urllib.parse.urlparse(self.path)
            path = (parsed.path or "/").rstrip("/") or "/"
            query = urllib.parse.parse_qs(parsed.query)

            if path == "/api/run-demo":
                ok, message = run_demo()
                data = {"ok": ok, "message": message}
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(json.dumps(data).encode("utf-8"))
                return
            if path == "/api/metrics":
                # refresh=1: run get_metrics() now (for button). Else return cache (updated every 3s by background thread).
                if query.get("refresh"):
                    fresh = get_metrics()
                    with _metrics_lock:
                        _metrics_cache.update(fresh)
                    data = fresh
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
    print("API: http://localhost:%d/api/metrics (add ?refresh=1 for live metrics)" % PORT)
    sys.stdout.flush()
    server.serve_forever()

if __name__ == "__main__":
    main()

