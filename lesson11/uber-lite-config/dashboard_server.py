#!/usr/bin/env python3
"""Lesson 11 Dashboard - Shows Config Architecture app output (health, config) at http://localhost:8085"""
import json
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

PORT = 8085
APP_BASE = "http://localhost:8080"
_REFRESH_INTERVAL_SEC = 3
_cache = {"health": None, "config": None, "error": None, "ts": 0}
_cache_lock = threading.Lock()


def _fetch(path):
    try:
        req = Request(APP_BASE + path, headers={"Accept": "application/json"})
        with urlopen(req, timeout=5) as r:
            return json.loads(r.read().decode())
    except (URLError, HTTPError, json.JSONDecodeError, OSError) as e:
        return {"_error": str(e)}


def _refresh_loop():
    global _cache
    while True:
        health = _fetch("/health")
        config = _fetch("/config")
        err = None
        if isinstance(health, dict) and "_error" in health:
            err = health.get("_error", "health failed")
        elif isinstance(config, dict) and "_error" in config:
            err = config.get("_error", "config failed")
        if err:
            health = health if isinstance(health, dict) and "_error" in health else health
            config = config if isinstance(config, dict) and "_error" in config else config
        with _cache_lock:
            _cache["health"] = health
            _cache["config"] = config
            _cache["error"] = err
            _cache["ts"] = time.time()
        time.sleep(_REFRESH_INTERVAL_SEC)


def get_cached():
    with _cache_lock:
        return {k: _cache[k] for k in ("health", "config", "error", "ts")}


HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Lesson 11 – Config Dashboard</title>
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <meta http-equiv="Pragma" content="no-cache">
  <meta http-equiv="Expires" content="0">
  <style>
    * { box-sizing: border-box; }
    body { font-family: 'Segoe UI', system-ui, sans-serif; margin: 0; padding: 1.5rem 2rem; color: #1a202c; background: linear-gradient(160deg, #0f172a 0%, #1e293b 50%, #0f3460 100%); min-height: 100vh; color: #e2e8f0; }
    h1 { color: #f8fafc; margin: 0 0 0.25rem 0; font-size: 1.75rem; font-weight: 600; }
    .sub { color: #94a3b8; font-size: 0.9rem; margin-bottom: 1.25rem; }
    .sub a { color: #38bdf8; text-decoration: none; }
    .sub a:hover { text-decoration: underline; }
    .cards { display: grid; grid-template-columns: 1fr 1fr; gap: 1.25rem; margin-top: 1rem; }
    @media (max-width: 768px) { .cards { grid-template-columns: 1fr; } }
    .card { background: rgba(30, 41, 59, 0.8); border: 1px solid #334155; border-radius: 10px; padding: 1.25rem; }
    .card h2 { margin: 0 0 0.75rem 0; font-size: 1rem; color: #94a3b8; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; }
    .json-block { font-family: 'JetBrains Mono', ui-monospace, monospace; font-size: 0.8rem; white-space: pre-wrap; word-break: break-all; background: #0f172a; border: 1px solid #334155; border-radius: 8px; padding: 1rem; margin: 0; overflow-x: auto; color: #cbd5e1; }
    .status-up { display: inline-block; padding: 0.25rem 0.6rem; background: #065f46; color: #6ee7b7; border-radius: 6px; font-weight: 600; font-size: 0.85rem; }
    .status-down { display: inline-block; padding: 0.25rem 0.6rem; background: #7f1d1d; color: #fca5a5; border-radius: 6px; font-weight: 600; font-size: 0.85rem; }
    .error-msg { color: #fca5a5; background: rgba(127, 29, 29, 0.3); border: 1px solid #b91c1c; border-radius: 8px; padding: 0.75rem 1rem; margin-bottom: 1rem; }
    .meta { font-size: 0.75rem; color: #64748b; margin-top: 0.5rem; }
    .badge { font-size: 0.7rem; color: #64748b; margin-left: 0.5rem; }
  </style>
</head>
<body>
  <h1>Lesson 11 – Config Architecture Dashboard</h1>
  <p class="sub">App: <a href="http://localhost:8080/health" target="_blank" rel="noopener">http://localhost:8080</a> &nbsp;|&nbsp; Auto-refresh every 3s</p>
  <div id="error" class="error-msg" style="display:none;"></div>
  <div class="cards">
    <div class="card">
      <h2>Health <span id="health-badge" class="badge"></span></h2>
      <div id="health-status"></div>
      <div id="health" class="json-block">Loading…</div>
      <p class="meta" id="health-meta"></p>
    </div>
    <div class="card">
      <h2>Config <span id="config-badge" class="badge"></span></h2>
      <div id="config" class="json-block">Loading…</div>
      <p class="meta" id="config-meta"></p>
    </div>
  </div>
  <script>
    var REFRESH_MS = 3000;
    function render(data) {
      var errEl = document.getElementById('error');
      var healthEl = document.getElementById('health');
      var configEl = document.getElementById('config');
      var healthStatus = document.getElementById('health-status');
      var healthMeta = document.getElementById('health-meta');
      var configMeta = document.getElementById('config-meta');
      var healthBadge = document.getElementById('health-badge');
      var configBadge = document.getElementById('config-badge');
      if (data.error) {
        errEl.style.display = 'block';
        errEl.textContent = 'App unreachable: ' + data.error + ' — ensure the app is running (mvn spring-boot:run -Dspring-boot.run.profiles=dev).';
        healthEl.textContent = typeof data.health === 'object' ? JSON.stringify(data.health, null, 2) : (data.health || '—');
        configEl.textContent = typeof data.config === 'object' ? JSON.stringify(data.config, null, 2) : (data.config || '—');
        healthStatus.innerHTML = '';
        healthBadge.textContent = '';
        configBadge.textContent = '';
        healthMeta.textContent = '';
        configMeta.textContent = '';
        return;
      }
      errEl.style.display = 'none';
      var h = data.health || {};
      var c = data.config || {};
      healthEl.textContent = JSON.stringify(h, null, 2);
      configEl.textContent = JSON.stringify(c, null, 2);
      var isUp = (h.status || '').toUpperCase() === 'UP';
      healthStatus.innerHTML = isUp ? '<span class="status-up">' + (h.status || 'UP') + '</span>' : '<span class="status-down">' + (h.status || 'DOWN') + '</span>';
      healthBadge.textContent = (h.profile && h.profile[0]) ? 'profile: ' + h.profile[0] : '';
      configBadge.textContent = c['spring.kafka.streams.application-id'] || '';
      var t = data.ts ? new Date(data.ts * 1000).toLocaleTimeString() : new Date().toLocaleTimeString();
      healthMeta.textContent = 'Updated: ' + t;
      configMeta.textContent = 'Updated: ' + t;
    }
    function load() {
      fetch('/api/output?t=' + Date.now(), { cache: 'no-store' })
        .then(function(r) { if (!r.ok) throw new Error(r.status); return r.json(); })
        .then(render)
        .catch(function(e) { render({ error: e.message, health: null, config: null }); });
    }
    load();
    setInterval(load, REFRESH_MS);
    document.addEventListener('visibilitychange', function() { if (document.visibilityState === 'visible') load(); });
  </script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            path = self.path.split("?")[0].rstrip("/") or "/"
            if path == "/api/output":
                data = get_cached()
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
                self.wfile.write(HTML.encode("utf-8"))
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
    daemon = threading.Thread(target=_refresh_loop, daemon=True)
    daemon.start()
    time.sleep(0.5)
    try:
        server = HTTPServer(("0.0.0.0", PORT), Handler)
    except OSError as e:
        if "Address already in use" in str(e) or getattr(e, "errno", None) == 98:
            sys.stderr.write("Port %d in use. Run: pkill -f dashboard_server.py\n" % PORT)
        else:
            sys.stderr.write("Failed to start: %s\n" % e)
        sys.exit(1)
    print("Lesson 11 dashboard: http://localhost:%d/dashboard" % PORT)
    print("(Fetches from app at %s; start the app if you see 'App unreachable'.)" % APP_BASE)
    sys.stdout.flush()
    server.serve_forever()


if __name__ == "__main__":
    main()
