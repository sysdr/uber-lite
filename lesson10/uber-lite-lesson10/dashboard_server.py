#!/usr/bin/env python3
"""Lesson 10 Dashboard - Dependency Management at http://localhost:8081"""
import json
import subprocess
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

PORT = 8081
SCRIPT_DIR = None  # set by main
_validation_cache = {"status": "idle", "output": "", "ts": 0}
_validation_lock = threading.Lock()

def run_validation():
    """Run DependencyValidator and return (success, output)."""
    global SCRIPT_DIR
    try:
        r = subprocess.run(
            ["./gradlew", "run", "--no-daemon", "-q"],
            capture_output=True, text=True, timeout=60,
            cwd=SCRIPT_DIR)
        out = (r.stdout or "") + (r.stderr or "")
        return (r.returncode == 0, out.strip() or "(no output)")
    except subprocess.TimeoutExpired:
        return (False, "Validation timed out (60s)")
    except Exception as e:
        return (False, str(e))

def get_metrics():
    """Return dashboard metrics (validation status and optional output)."""
    with _validation_lock:
        return {
            "validationStatus": _validation_cache.get("status", "idle"),
            "validationOutput": _validation_cache.get("output", ""),
            "ts": _validation_cache.get("ts", 0),
        }

def _refresh_validation():
    """Background: run validation and update cache (optional, or on-demand only)."""
    global _validation_cache
    with _validation_lock:
        if _validation_cache.get("status") == "running":
            return
        _validation_cache = {"status": "running", "output": "...", "ts": time.time()}
    ok, out = run_validation()
    with _validation_lock:
        _validation_cache = {
            "status": "passed" if ok else "failed",
            "output": out,
            "ts": time.time(),
        }

HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Lesson 10 – Dependency Management Dashboard</title>
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
  <style>
    body { font-family: system-ui, sans-serif; margin: 2rem; color: #1a202c; }
    h1 { color: #0f3460; }
    .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 1rem; margin-top: 1rem; }
    .card { background: #fff; border: 1px solid #e5e7eb; padding: 1rem; border-radius: 8px; }
    .card .val { font-size: 1.2rem; font-weight: 600; }
    .passed { color: #10b981; }
    .failed { color: #e94560; }
    .idle { color: #718096; }
    pre { background: #f3f4f6; padding: 0.75rem; border-radius: 6px; overflow-x: auto; font-size: 0.85rem; white-space: pre-wrap; }
    button { margin-top: 0.5rem; padding: 0.5rem 1rem; cursor: pointer; background: #10b981; color: #fff; border: none; border-radius: 6px; }
    button:hover { background: #059669; }
    html, body { min-height: 100vh; background: #e6f4f1; }
  </style>
</head>
<body>
  <h1>Lesson 10 – Dependency Management Dashboard</h1>
  <p class="status">Validation: <span id="status">—</span> &nbsp; <button id="runBtn">Run validation now</button></p>
  <div class="cards">
    <div class="card">
      <h3>RocksDB / H3</h3>
      <div id="summary">Run validation to check native libraries.</div>
    </div>
    <div class="card">
      <h3>Last output</h3>
      <pre id="output">(none yet)</pre>
    </div>
  </div>
  <script>
    var runBtn = document.getElementById('runBtn');
    var statusEl = document.getElementById('status');
    var summaryEl = document.getElementById('summary');
    var outputEl = document.getElementById('output');

    function setStatus(s, cls) {
      statusEl.textContent = s;
      statusEl.className = cls || '';
    }

    function fetchMetrics() {
      fetch('/api/metrics?t=' + Date.now(), { cache: 'no-store' })
        .then(function(r) { return r.json(); })
        .then(function(d) {
          var s = d.validationStatus || 'idle';
          setStatus(s, s === 'passed' ? 'passed' : s === 'failed' ? 'failed' : 'idle');
          summaryEl.textContent = s === 'passed' ? 'All validations passed' : s === 'failed' ? 'Validation failed' : 'Run validation to check native libraries.';
          outputEl.textContent = d.validationOutput || '(none yet)';
        })
        .catch(function() { setStatus('error', 'failed'); });
    }

    runBtn.onclick = function() {
      runBtn.disabled = true;
      setStatus('running...', 'idle');
      outputEl.textContent = 'Running validation (may take 15–20s)...';
      fetch('/api/run-validation', { method: 'POST', cache: 'no-store' })
        .then(function(r) { return r.json(); })
        .then(function(d) {
          var s = d.validationStatus || 'idle';
          setStatus(s, s === 'passed' ? 'passed' : s === 'failed' ? 'failed' : 'idle');
          summaryEl.textContent = s === 'passed' ? 'All validations passed' : s === 'failed' ? 'Validation failed' : 'Run validation to check native libraries.';
          outputEl.textContent = d.validationOutput || '(none yet)';
        })
        .catch(function() { setStatus('error', 'failed'); outputEl.textContent = 'Request failed'; })
        .finally(function() { runBtn.disabled = false; });
    };

    setInterval(fetchMetrics, 3000);
    fetchMetrics();
  </script>
</body>
</html>"""

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?")[0].rstrip("/") or "/"
        try:
            if path == "/api/metrics":
                data = get_metrics()
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
            self.send_response(404)
            self.end_headers()
        except Exception:
            pass

    def do_POST(self):
        path = self.path.split("?")[0].rstrip("/") or "/"
        if path == "/api/run-validation":
            _refresh_validation()
            data = get_metrics()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode("utf-8"))
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        pass

def main():
    global SCRIPT_DIR
    import os
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    # Dashboard runs from lesson10/ but validation runs in uber-lite-lesson10/
    project_dir = os.path.join(SCRIPT_DIR, "uber-lite-lesson10")
    if os.path.isdir(project_dir):
        SCRIPT_DIR = project_dir
    try:
        server = HTTPServer(("0.0.0.0", PORT), Handler)
    except OSError as e:
        if "Address already in use" in str(e) or e.errno == 98:
            sys.stderr.write("Port %d in use. Run: pkill -f dashboard_server.py\n" % PORT)
        else:
            sys.stderr.write("Failed to start: %s\n" % e)
        sys.exit(1)
    print("Lesson 10 dashboard: http://localhost:%d/dashboard" % PORT)
    print("API: http://localhost:%d/api/metrics" % PORT)
    sys.stdout.flush()
    server.serve_forever()

if __name__ == "__main__":
    main()
