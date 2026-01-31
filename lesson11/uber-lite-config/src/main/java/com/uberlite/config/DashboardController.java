package com.uberlite.config;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DashboardController {

    private static final String HTML = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>Lesson 11 - Config Dashboard</title>
          <meta http-equiv="Cache-Control" content="no-cache">
          <style>
            * { box-sizing: border-box; }
            body { font-family: system-ui, sans-serif; margin: 0; padding: 1.5rem 2rem; color: #1a202c; background: #f1f5f9; }
            h1 { color: #0f3460; margin: 0 0 0.5rem 0; font-size: 1.5rem; }
            .links { margin-bottom: 1.5rem; }
            .links a { color: #2563eb; text-decoration: none; margin-right: 1rem; }
            .links a:hover { text-decoration: underline; }
            .card { background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 1.25rem; margin-bottom: 1rem; }
            .card h2 { margin: 0 0 0.75rem 0; font-size: 1.1rem; color: #475569; }
            .json-block { font-family: ui-monospace, monospace; font-size: 0.875rem; white-space: pre-wrap; word-break: break-all; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 1rem; margin: 0; overflow-x: auto; }
            .status-up { display: inline-block; padding: 0.25rem 0.5rem; background: #dcfce7; color: #166534; border-radius: 4px; font-weight: 600; }
            .btn { padding: 0.5rem 1rem; background: #0f3460; color: #fff; border: none; border-radius: 6px; cursor: pointer; font-size: 0.875rem; }
            .btn:hover { background: #1e40af; }
            .meta { font-size: 0.8rem; color: #64748b; margin-top: 0.5rem; }
          </style>
        </head>
        <body>
          <h1>Lesson 11 - Configuration Dashboard</h1>
          <p class="links"><a href="/health">/health</a><a href="/config">/config</a><button class="btn" onclick="load()">Refresh</button></p>
          <div class="card">
            <h2>Health</h2>
            <div id="health-status"></div>
            <div id="health" class="json-block">Loading...</div>
            <p class="meta" id="health-meta"></p>
          </div>
          <div class="card">
            <h2>Config</h2>
            <div id="config" class="json-block">Loading...</div>
            <p class="meta" id="config-meta"></p>
          </div>
          <script>
            function load() {
              var t = new Date().toLocaleTimeString();
              fetch('/health', { cache: 'no-store' }).then(function(r) { if (!r.ok) throw new Error(r.status); return r.json(); }).then(function(d) {
                document.getElementById('health').textContent = JSON.stringify(d, null, 2);
                document.getElementById('health-status').innerHTML = (d.status === 'UP') ? '<span class="status-up">' + d.status + '</span>' : '';
                document.getElementById('health-meta').textContent = 'Last updated: ' + t;
              }).catch(function(e) { document.getElementById('health').textContent = 'Error: ' + e; document.getElementById('health-status').innerHTML = ''; });
              fetch('/config', { cache: 'no-store' }).then(function(r) { if (!r.ok) throw new Error(r.status); return r.json(); }).then(function(d) {
                document.getElementById('config').textContent = JSON.stringify(d, null, 2);
                document.getElementById('config-meta').textContent = 'Last updated: ' + t;
              }).catch(function(e) { document.getElementById('config').textContent = 'Error: ' + e; });
            }
            load();
          </script>
        </body>
        </html>
        """;

    private static ResponseEntity<String> dashboardResponse() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.parseMediaType("text/html;charset=UTF-8"));
        return new ResponseEntity<>(HTML, headers, HttpStatus.OK);
    }

    @GetMapping(path = "/", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> root() {
        return dashboardResponse();
    }

    @GetMapping(path = "/dashboard", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> dashboard() {
        return dashboardResponse();
    }
}
