#!/usr/bin/env python3
"""
Lesson 8 Dashboard - Serves cluster metrics at http://localhost:8080
Run: ./start-dashboard.sh or python3 dashboard_server.py
Requires: cluster running (./start.sh), run produce-demo.sh for non-zero counts.
"""
import json
import subprocess
import sys
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen
from urllib.error import URLError

PORT = 8080


def _topic_offset(topic):
    """Fetch total offset for one topic; returns 0 on any failure."""
    try:
        r = subprocess.run(
            ["docker", "exec", "kafka-1", "kafka-run-class", "kafka.tools.GetOffsetShell",
             "--broker-list", "localhost:9092", "--topic", topic],
            capture_output=True, text=True, timeout=10)
        total = 0
        for line in (r.stdout or "").strip().splitlines():
            parts = line.strip().split(":")
            if len(parts) >= 3:
                try:
                    total += int(parts[2])
                except ValueError:
                    pass
        return total
    except Exception:
        return 0


def get_metrics():
    """Collect cluster metrics; never raises, always returns a dict."""
    out = {
        "brokerCount": 0,
        "containerCount": 0,
        "driverLocationsTotal": 0,
        "riderLocationsTotal": 0,
        "rideMatchesTotal": 0,
        "schemaRegistryReachable": False,
        "error": None,
    }
    try:
        # Container count (no dependency on Kafka)
        try:
            r = subprocess.run(
                ["docker", "ps", "--filter", "name=kafka-", "--filter", "name=zookeeper",
                 "--filter", "name=schema-registry", "--format", "{{.Names}}"],
                capture_output=True, text=True, timeout=5)
            names = [n for n in (r.stdout or "").strip().splitlines() if n]
            out["containerCount"] = len(names)
            out["brokerCount"] = sum(1 for n in names if n.startswith("kafka-"))
        except Exception:
            pass

        # Topic offsets (each topic isolated so one failure doesn't break others)
        out["driverLocationsTotal"] = _topic_offset("driver-locations")
        out["riderLocationsTotal"] = _topic_offset("rider-locations")
        out["rideMatchesTotal"] = _topic_offset("ride-matches")

        # Schema Registry (no curl dependency; use urllib)
        try:
            urlopen("http://localhost:8081/", timeout=3).close()
            out["schemaRegistryReachable"] = True
        except (URLError, OSError, Exception):
            out["schemaRegistryReachable"] = False
    except Exception as e:
        out["error"] = str(e)
    out["ts"] = time.time()
    return out


HTML = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Lesson 8 – Cluster Dashboard</title>
  <meta http-equiv="refresh" content="300">
  <style>
    body { font-family: system-ui,sans-serif; margin: 2rem; background-color: #F5F7FA; color: #1a202c; }
    h1 { color: #0f3460; margin-bottom: 0.25rem; }
    .sub { color: #4a5568; font-size: 0.9rem; margin-bottom: 1rem; }
    .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 1rem; margin-top: 1rem; }
    .card { background-color: #FFFFFF; border: 1px solid #E5E7EB; box-shadow: 0 4px 10px rgba(0, 0, 0, 0.06); padding: 1rem; border-radius: 8px; }
    .card h3 { margin: 0 0 0.5rem 0; font-size: 0.9rem; color: #1F2937; }
    .card .val { font-size: 1.8rem; font-weight: 700; color: #10B981; }
    .card.live .val { color: #10B981; }
    .card:last-child .val { color: #059669; }
    .error { color: #e94560; }
    .live-indicator { display: inline-block; width: 8px; height: 8px; background: #4ade80; border-radius: 50%; animation: pulse 2s infinite; margin-right: 6px; }
    .refreshing { opacity: 0.6; }
    #refreshStatus { font-weight: bold; }
    @keyframes pulse { 0%,100%{ opacity:1 } 50%{ opacity:0.5 } }
  </style>
</head>
<body>
  <h1>Lesson 8 – Kafka Cluster Dashboard</h1>
  <p class="sub"><span class="live-indicator"></span> Real-time metrics (every 3s) &middot; <span id="liveClock">Live: --:--:--</span> &middot; <span id="lastUpdated">--</span> &middot; <span id="refreshStatus">--</span></p>
  <p class="sub" style="font-size:0.75rem;color:#718096;"><span id="debug">Debug: --</span></p>
  <p id="hint" class="sub" style="margin-bottom:0.5rem;"></p>
  <div id="root">Loading…</div>
  <script>
    (function() {
      const REFRESH_MS = 3000;
      var refreshCount = 0;

      function render(data) {
        var total = (data.driverLocationsTotal||0)+(data.riderLocationsTotal||0)+(data.rideMatchesTotal||0);
        var hintEl = document.getElementById('hint');
        if (total === 0 && !data.error) {
          hintEl.innerHTML = 'To add sample data, run: <code>./produce-demo.sh</code>';
          hintEl.style.display = 'block';
        } else if (total > 0) {
          hintEl.innerHTML = '';
          hintEl.style.display = 'none';
        } else if (data.error) {
          hintEl.innerHTML = '';
          hintEl.style.display = 'none';
        }
        var err = data.error ? '<p class="error">' + data.error + '</p>' : '';
        var t = total > 0 ? ' live' : '';
        document.getElementById('root').innerHTML = err + [
          '<div class="cards">',
          '<div class="card"><h3>Containers</h3><div class="val" id="val-containers">0</div></div>',
          '<div class="card"><h3>Brokers</h3><div class="val" id="val-brokers">0</div></div>',
          '<div class="card' + t + '"><h3>driver-locations</h3><div class="val" id="val-driver">0</div></div>',
          '<div class="card' + t + '"><h3>rider-locations</h3><div class="val" id="val-rider">0</div></div>',
          '<div class="card' + t + '"><h3>ride-matches</h3><div class="val" id="val-matches">0</div></div>',
          '<div class="card"><h3>Schema Registry</h3><div class="val" id="val-schema">–</div></div>',
          '</div>'
        ].join('');
        var c = document.getElementById('val-containers');
        var b = document.getElementById('val-brokers');
        var dr = document.getElementById('val-driver');
        var rr = document.getElementById('val-rider');
        var m = document.getElementById('val-matches');
        var s = document.getElementById('val-schema');
        if (c) c.textContent = data.containerCount != null ? data.containerCount : 0;
        if (b) b.textContent = data.brokerCount != null ? data.brokerCount : 0;
        if (dr) dr.textContent = data.driverLocationsTotal != null ? data.driverLocationsTotal : 0;
        if (rr) rr.textContent = data.riderLocationsTotal != null ? data.riderLocationsTotal : 0;
        if (m) m.textContent = data.rideMatchesTotal != null ? data.rideMatchesTotal : 0;
        if (s) s.textContent = data.schemaRegistryReachable ? 'OK' : '–';
        document.getElementById('lastUpdated').textContent = 'Updated: ' + new Date().toLocaleTimeString();
      }

      function fetchMetrics() {
        refreshCount++;
        var statusEl = document.getElementById('refreshStatus');
        if (statusEl) statusEl.textContent = 'Refreshing...';
        var url = '/api/metrics?t=' + Date.now() + '&r=' + Math.random();
        fetch(url, { cache: 'no-store', headers: { 'Cache-Control': 'no-cache', 'Pragma': 'no-cache' } })
          .then(function(res) {
            var statusEl = document.getElementById('refreshStatus');
            if (!res.ok) {
              if (statusEl) statusEl.textContent = 'Error ' + res.status;
              document.getElementById('root').innerHTML = '<p class="error">HTTP ' + res.status + '</p>';
              return;
            }
            return res.json();
          })
          .then(function(d) {
            if (!d) return;
            console.log('Metrics received #' + refreshCount + ':', d);
            var debugEl = document.getElementById('debug');
            var serverTime = d.ts != null ? new Date(d.ts * 1000).toLocaleTimeString() : '';
            if (debugEl) debugEl.textContent = 'Last: driver=' + (d.driverLocationsTotal||0) + ' rider=' + (d.riderLocationsTotal||0) + ' matches=' + (d.rideMatchesTotal||0) + (serverTime ? ' (server ' + serverTime + ')' : '');
            render(d);
            var statusEl = document.getElementById('refreshStatus');
            if (statusEl) statusEl.textContent = 'Refreshed #' + refreshCount + ' at ' + new Date().toLocaleTimeString();
          })
          .catch(function(err) {
            var statusEl = document.getElementById('refreshStatus');
            if (statusEl) statusEl.textContent = 'Network error';
            document.getElementById('root').innerHTML = '<p class="error">Network error</p>';
          });
      }

      function tickClock() {
        var el = document.getElementById('liveClock');
        if (el) el.textContent = 'Live: ' + new Date().toLocaleTimeString();
      }

      tickClock();
      setInterval(tickClock, 1000);
      fetchMetrics();
      setInterval(fetchMetrics, REFRESH_MS);
    })();
  </script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            if self.path == "/api/metrics" or self.path.startswith("/api/metrics?"):
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
                self.end_headers()
                self.wfile.write(json.dumps(get_metrics(), ensure_ascii=False).encode("utf-8"))
                return
            if self.path in ("/", "/dashboard"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
                self.end_headers()
                self.wfile.write(HTML.encode("utf-8"))
                return
            self.send_response(404)
            self.end_headers()
        except (BrokenPipeError, ConnectionResetError):
            pass  # Client disconnected; ignore
        except Exception as e:
            try:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))
            except Exception:
                pass

    def log_message(self, format, *args):
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), format % args))


def main():
    try:
        server = HTTPServer(("", PORT), Handler)
    except OSError as e:
        if "Address already in use" in str(e) or e.errno == 98:
            sys.stderr.write("Port %d already in use. Stop the other process or use a different port.\n" % PORT)
        else:
            sys.stderr.write("Failed to start server: %s\n" % e)
        sys.exit(1)
    print("Dashboard: http://localhost:%d/dashboard" % PORT)
    print("API:       http://localhost:%d/api/metrics" % PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()


if __name__ == "__main__":
    main()
