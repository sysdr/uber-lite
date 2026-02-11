#!/usr/bin/env python3
"""Lesson 20 Dashboard - Serves cluster metrics at http://localhost:8080"""
import json
import subprocess
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

PORT = 8080
KAFKA_CONTAINER = "lesson-20-concurrency-kafka-1"
BROKER_LIST = "kafka:9092"
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
            ["docker", "ps", "--filter", "name=lesson-20-concurrency-", "--format", "{{.Names}}"],
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

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Lesson 20 – Concurrency Dashboard</title>
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <meta http-equiv="Pragma" content="no-cache">
  <meta http-equiv="Expires" content="0">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700&family=JetBrains+Mono:wght@500;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #0a0e14;
      --bg-mid: #0d1219;
      --surface: rgba(22,27,34,0.85);
      --surface-solid: #161b22;
      --surface-hover: #1c2128;
      --border: rgba(48,54,61,0.8);
      --text: #e6edf3;
      --text-muted: #7d8590;
      --accent: #3fb950;
      --accent-glow: rgba(63,185,80,0.25);
      --accent-dim: #238636;
      --cyan: #58a6ff;
      --cyan-dim: rgba(88,166,255,0.15);
      --violet: #a371f7;
      --violet-dim: rgba(163,113,247,0.12);
      --amber: #d29922;
      --red: #f85149;
      --radius: 16px;
      --radius-sm: 10px;
      --shadow: 0 12px 40px rgba(0,0,0,0.35);
      --shadow-glow: 0 0 40px var(--accent-glow);
    }
    * { box-sizing: border-box; }
    html, body { min-height: 100vh; margin: 0; }
    body {
      font-family: 'Outfit', system-ui, sans-serif;
      background: var(--bg);
      color: var(--text);
      padding: 2rem;
      background-image:
        radial-gradient(ellipse 100% 60% at 50% -15%, rgba(63,185,80,0.08), transparent 50%),
        radial-gradient(ellipse 70% 50% at 90% 20%, rgba(88,166,255,0.05), transparent 45%),
        radial-gradient(ellipse 50% 30% at 10% 80%, rgba(163,113,247,0.04), transparent 40%);
    }
    .wrap {
      max-width: 960px;
      margin: 0 auto;
    }
    .header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 1rem;
      margin-bottom: 2rem;
      padding-bottom: 1.5rem;
      border-bottom: 1px solid var(--border);
    }
    .header-left h1 {
      font-size: 1.85rem;
      font-weight: 700;
      letter-spacing: -0.03em;
      margin: 0 0 0.25rem 0;
      color: var(--text);
    }
    .header-left .sub {
      font-size: 0.9rem;
      color: var(--text-muted);
      font-weight: 500;
    }
    .live-badge {
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      font-size: 0.8rem;
      font-weight: 600;
      padding: 0.4rem 0.9rem;
      border-radius: 999px;
      background: linear-gradient(135deg, var(--accent-dim) 0%, #2ea043 100%);
      color: #fff;
      border: 1px solid rgba(63,185,80,0.3);
      box-shadow: 0 0 20px var(--accent-glow);
    }
    .live-badge .dot {
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: #fff;
      animation: pulse 1.5s ease-in-out infinite;
    }
    @keyframes pulse {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.6; transform: scale(1.2); }
    }
    #hint {
      font-size: 0.9rem;
      color: var(--text-muted);
      margin: 0 0 1.25rem 0;
    }
    #hint code {
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.85em;
      padding: 0.25rem 0.5rem;
      background: var(--surface-hover);
      border-radius: 8px;
      color: var(--cyan);
    }
    .cards {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 1.5rem;
    }
    @media (max-width: 700px) {
      .cards { grid-template-columns: 1fr; }
    }
    .card {
      background: var(--surface);
      backdrop-filter: blur(12px);
      -webkit-backdrop-filter: blur(12px);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 1.75rem;
      transition: border-color 0.25s, transform 0.25s, box-shadow 0.25s;
      animation: cardIn 0.5s ease backwards;
    }
    .card:nth-child(1) { animation-delay: 0.05s; }
    .card:nth-child(2) { animation-delay: 0.1s; }
    .card:nth-child(3) { animation-delay: 0.15s; }
    @keyframes cardIn {
      from { opacity: 0; transform: translateY(12px); }
      to { opacity: 1; transform: translateY(0); }
    }
    .card:hover {
      border-color: var(--text-muted);
      box-shadow: var(--shadow);
      transform: translateY(-2px);
    }
    .card.hero {
      border-color: rgba(63,185,80,0.4);
      background: linear-gradient(145deg, var(--surface) 0%, rgba(35,134,54,0.12) 100%);
      box-shadow: 0 0 0 1px rgba(63,185,80,0.15), var(--shadow);
      animation-delay: 0.2s;
    }
    .card.hero:hover {
      box-shadow: 0 0 0 1px rgba(63,185,80,0.2), var(--shadow-glow), var(--shadow);
    }
    .card-icon {
      width: 40px;
      height: 40px;
      border-radius: var(--radius-sm);
      display: flex;
      align-items: center;
      justify-content: center;
      margin-bottom: 1rem;
      font-size: 1.25rem;
    }
    .card .icon-wrap { margin-bottom: 1rem; }
    .card .icon-box { background: var(--cyan-dim); color: var(--cyan); }
    .card .icon-broker { background: var(--violet-dim); color: var(--violet); }
    .card.hero .icon-hero { background: rgba(63,185,80,0.2); color: var(--accent); }
    .card h3 {
      font-size: 0.75rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      color: var(--text-muted);
      margin: 0 0 0.5rem 0;
    }
    .card .val {
      font-family: 'JetBrains Mono', monospace;
      font-size: 2.1rem;
      font-weight: 600;
      color: var(--cyan);
      letter-spacing: -0.02em;
      line-height: 1.2;
    }
    .card.hero .val {
      font-size: 2.5rem;
      color: var(--accent);
      text-shadow: 0 0 30px var(--accent-glow);
    }
    .card .sub {
      font-size: 0.8rem;
      color: var(--text-muted);
      margin-top: 0.35rem;
    }
    .status-bar {
      margin-top: 2rem;
      padding-top: 1rem;
      border-top: 1px solid var(--border);
      font-size: 0.8rem;
      color: var(--text-muted);
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }
    .status-bar .sync {
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: var(--accent);
      flex-shrink: 0;
    }
    .error {
      background: rgba(248,81,73,0.1);
      border: 1px solid var(--red);
      color: var(--red);
      padding: 1.25rem 1.5rem;
      border-radius: var(--radius);
      font-size: 0.9rem;
    }
    #root { min-height: 140px; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <div class="header-left">
        <h1>Lesson 20 – Concurrency</h1>
        <p class="sub">Virtual threads &middot; Kafka metrics</p>
      </div>
      <span class="live-badge"><span class="dot"></span> Live</span>
    </div>
    <p id="hint"></p>
    <div id="root">Loading…</div>
    <p id="status" class="status-bar"><span class="sync"></span> Loading…</p>
  </div>
  <script>
    var REFRESH_INTERVAL_MS = 2000;
    var refreshCount = 0;
    function formatNum(n) {
      if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B';
      if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
      if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
      return String(n);
    }
    var iconBox = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>';
    var iconBroker = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/><path d="M3 12c0 1.66 4 3 9 3s9-1.34 9-3"/></svg>';
    var iconHero = '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>';
    function render(d) {
      var root = document.getElementById('root');
      var hint = document.getElementById('hint');
      var status = document.getElementById('status');
      if (d && d.error) {
        hint.innerHTML = '';
        root.innerHTML = '<p class="error">Server: ' + (d.error || 'Unknown error') + '</p>';
        status.innerHTML = '<span class="sync"></span> Last update: error';
        return;
      }
      if (!d) {
        root.innerHTML = '<p class="error">Invalid response</p>';
        status.innerHTML = '<span class="sync"></span> Last update: error';
        return;
      }
      if ((d.driverLocationsTotal||0) === 0 && !d.error)
        hint.innerHTML = 'Run <code>./demo.sh</code> to produce events. Dashboard refreshes every ' + (REFRESH_INTERVAL_MS/1000) + 's.';
      else
        hint.innerHTML = '';
      var total = d.driverLocationsTotal || 0;
      root.innerHTML = [
        '<div class="cards">',
        '<div class="card"><div class="card-icon icon-wrap icon-box">' + iconBox + '</div><h3>Containers</h3><div class="val">' + (d.containerCount||0) + '</div></div>',
        '<div class="card"><div class="card-icon icon-wrap icon-broker">' + iconBroker + '</div><h3>Brokers</h3><div class="val">' + (d.brokerCount||0) + '</div></div>',
        '<div class="card hero"><div class="card-icon icon-wrap icon-hero">' + iconHero + '</div><h3>driver-locations</h3><div class="val">' + formatNum(total) + '</div><div class="sub">' + (total >= 1000 ? total.toLocaleString() + ' records' : '') + '</div></div>',
        '</div>'
      ].join('');
      status.innerHTML = '<span class="sync"></span> Last updated ' + new Date().toLocaleTimeString() + ' &middot; refresh #' + refreshCount + ' &middot; every ' + (REFRESH_INTERVAL_MS/1000) + 's';
    }
    function load() {
      var url = '/api/metrics?t=' + Date.now() + '&r=' + Math.random();
      fetch(url, { cache: 'no-store', headers: { 'Cache-Control': 'no-cache', 'Pragma': 'no-cache' } })
        .then(function(r) { if (!r.ok) throw new Error('API ' + r.status); return r.json(); })
        .then(function(d) { refreshCount++; render(d); })
        .catch(function(err) {
          var root = document.getElementById('root');
          var status = document.getElementById('status');
          if (status) status.innerHTML = '<span class="sync"></span> Last update failed – ' + (err.message || 'Network error');
          if (root) root.innerHTML = '<p class="error">' + (err.message || 'Network error') + ' (retrying…)</p>';
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
    print("API: http://localhost:%d/api/metrics" % PORT)
    sys.stdout.flush()
    server.serve_forever()

if __name__ == "__main__":
    main()
