#!/usr/bin/env python3
"""
dashboard.py  -  one-command demo launcher + web frontend for the load balancer project.

    python dashboard.py            (opens http://127.0.0.1:5000 in your browser)

What it does
------------
* Starts the three backend servers (web-1 :9001, web-2 :9002, web-3 :9003 slow) and the
  load balancer (:8080) as child processes, exactly as you would in five terminals.
* Reads the load balancer's log in real time (health changes, retries, every connection).
* Polls the load balancer's stats endpoint (:8081) for UP/DOWN state and counters.
* Generates client traffic itself (continuous "live traffic" or a one-off burst test),
  so the dashboard can show which backend served each request, per second.
* Serves dashboard.html plus a small JSON API the page calls:

      GET  /api/state                    everything the page draws (polled every 0.5 s)
      POST /api/backend/<port>/stop      kill a backend process   (simulates a crash)
      POST /api/backend/<port>/start     start it again
      POST /api/algo      {"algo": "rr" | "least" | "hash"}   restart the LB with that algorithm
      POST /api/traffic   {"running": true, "rate": 5}        live traffic on/off, requests per second
      POST /api/burst     {"n": 60, "c": 6}                    run a burst test, returns the results
      POST /api/reset                                          clear counters and chart

Everything uses the Python standard library, and the page loads nothing from the
Internet, so the demo works in a lab with no network access.
Stop everything with Ctrl+C (all child processes are stopped too).
"""

import argparse
import atexit
import json
import math
import os
import re
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
import webbrowser
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from client_loadgen import one_request  # noqa: E402  (reuse the exact same client code)

PY = sys.executable
LB_HOST, LB_PORT, LB_ADMIN = "127.0.0.1", 8080, 8081
BACKENDS = [  # name, port, artificial delay in ms
    {"name": "web-1", "port": 9001, "delay": 0},
    {"name": "web-2", "port": 9002, "delay": 0},
    {"name": "web-3", "port": 9003, "delay": 150},
]
# In "hash" mode the traffic generator pretends to be 5 different clients by binding
# to different loopback source addresses (works on Windows and Linux; on macOS only
# 127.0.0.1 exists, so it silently falls back to one client).
HASH_CLIENTS = ["127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"]

CREATE_FLAGS = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0


def now_str():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


class Demo:
    """Owns the child processes and all state the page shows."""

    def __init__(self, interval):
        self.lock = threading.Lock()
        self.interval = interval
        self.algo = "rr"
        self.procs = {}                 # port -> Popen for backends
        self.lb = None                  # Popen for the load balancer
        self.events = deque(maxlen=60)  # HEALTH / RETRY / DROP / info lines
        self.conns = deque(maxlen=14)   # most recent CONN lines, parsed
        self.timeline = {}              # int(second) -> {"web-1": n, ..., "fail": n, "lat": [..]}
        self.ok = 0
        self.fail = 0
        self.traffic_running = False
        self.traffic_rate = 4.0
        self.last_burst = None
        self.client_map = {}            # source ip -> last backend (hash demo)
        self.hash_ips_ok = None         # None = untested, True/False after first try
        self.pool = ThreadPoolExecutor(max_workers=48)

    # ---------------------------------------------------------------- processes
    def start_backend(self, port):
        b = next(x for x in BACKENDS if x["port"] == port)
        p = self.procs.get(port)
        if p and p.poll() is None:
            return
        cmd = [PY, "-u", os.path.join(HERE, "backend_server.py"), "--port", str(port), "--name", b["name"]]
        if b["delay"]:
            cmd += ["--delay", str(b["delay"])]
        self.procs[port] = subprocess.Popen(cmd, cwd=HERE, stdout=subprocess.DEVNULL,
                                            stderr=subprocess.DEVNULL, creationflags=CREATE_FLAGS)
        self.event("info", f"{b['name']} (:{port}) process started")

    def stop_backend(self, port):
        p = self.procs.get(port)
        if p and p.poll() is None:
            p.kill()          # abrupt, like a crash: the OS closes the listening socket
            p.wait(timeout=5)
            name = next(x["name"] for x in BACKENDS if x["port"] == port)
            self.event("crash", f"{name} (:{port}) process killed - simulating a server crash")

    def start_lb(self):
        self.stop_lb()
        cmd = [PY, "-u", os.path.join(HERE, "load_balancer.py"), "--algo", self.algo,
               "--interval", str(self.interval), "--listen", f"{LB_HOST}:{LB_PORT}",
               "--admin", f"{LB_HOST}:{LB_ADMIN}"]
        self.lb = subprocess.Popen(cmd, cwd=HERE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, bufsize=1, creationflags=CREATE_FLAGS)
        threading.Thread(target=self._read_lb_log, args=(self.lb,), daemon=True).start()
        # wait until the LB is accepting connections
        for _ in range(50):
            try:
                socket.create_connection((LB_HOST, LB_PORT), timeout=0.2).close()
                break
            except OSError:
                time.sleep(0.1)

    def stop_lb(self):
        if self.lb and self.lb.poll() is None:
            self.lb.kill()
            self.lb.wait(timeout=5)
        self.lb = None

    def shutdown(self):
        self.traffic_running = False
        try:
            self.stop_lb()
        except Exception:
            pass
        for p in list(self.procs.values()):
            if p.poll() is None:
                p.kill()

    # ---------------------------------------------------------------- LB log parsing
    CONN_RE = re.compile(r"^\[(?P<t>[\d:.]+)\] CONN\s+(?P<client>\S+) -> (?P<backend>\S+)\s+\[(?P<algo>\w+)\]"
                         r"\s+up (?P<up>\d+) B / down (?P<down>\d+) B\s+(?P<ms>[\d.]+) ms")

    def _read_lb_log(self, proc):
        for line in proc.stdout:
            line = line.rstrip()
            m = self.CONN_RE.match(line)
            if m:
                port = int(m["backend"].rsplit(":", 1)[1])
                name = next((b["name"] for b in BACKENDS if b["port"] == port), m["backend"])
                with self.lock:
                    self.conns.appendleft({"time": m["t"], "client": m["client"], "backend": name,
                                           "port": port, "up": int(m["up"]), "down": int(m["down"]),
                                           "ms": float(m["ms"])})
                continue
            for kind in ("HEALTH", "RETRY", "DROP"):
                if f"] {kind}" in line:
                    ts = line[1:13]
                    msg = line.split(f"] {kind}", 1)[1].strip()
                    for b in BACKENDS:   # show names instead of 127.0.0.1:900x
                        msg = msg.replace(f"127.0.0.1:{b['port']}", f"{b['name']} (:{b['port']})")
                    kind_l = kind.lower()
                    if kind == "HEALTH":
                        kind_l = "up" if "UP again" in msg else "down"
                    self.event(kind_l, msg, ts)

    def event(self, kind, msg, ts=None):
        with self.lock:
            self.events.appendleft({"time": ts or now_str(), "kind": kind, "msg": msg})

    # ---------------------------------------------------------------- traffic
    def _record(self, ok, who, ms, src, live=True):
        sec = int(time.time())
        with self.lock:
            if src and ok:
                self.client_map[src] = who
            if not live:          # burst tests are reported in their own panel, not on the live chart
                return
            bucket = self.timeline.setdefault(sec, {"fail": 0, "lat": []})
            if ok:
                self.ok += 1
                bucket[who] = bucket.get(who, 0) + 1
                bucket["lat"].append(ms)
            else:
                self.fail += 1
                bucket["fail"] += 1
            for old in [s for s in self.timeline if s < sec - 90]:
                del self.timeline[old]

    def _source_ip(self, i):
        if self.algo != "hash" or self.hash_ips_ok is False:
            return None
        return HASH_CLIENTS[i % len(HASH_CLIENTS)]

    def _one(self, i, live=True):
        src = self._source_ip(i)
        ok, who, ms = one_request(LB_HOST, LB_PORT, timeout=4.0, source_ip=src)
        if not ok and src and src != "127.0.0.1" and who == "OSError":
            self.hash_ips_ok = False           # this OS can't bind 127.0.0.x - fall back
            ok, who, ms = one_request(LB_HOST, LB_PORT, timeout=4.0)
            src = None
        elif ok and src:
            self.hash_ips_ok = True
        self._record(ok, who, ms, src or ("127.0.0.1" if self.algo == "hash" else None), live)
        return ok, who, ms

    def traffic_loop(self):
        i = 0
        while True:
            if self.traffic_running:
                self.pool.submit(self._one, i)
                i += 1
                time.sleep(1.0 / max(0.5, self.traffic_rate))
            else:
                time.sleep(0.1)

    def burst(self, n, c):
        n = max(1, min(int(n), 2000))
        c = max(1, min(int(c), 32))
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=c) as ex:
            results = list(ex.map(lambda i: self._one(i, live=False), range(n)))
        wall = time.perf_counter() - t0
        ok = [r for r in results if r[0]]
        lat = sorted(r[2] for r in ok)
        dist = Counter(r[1] for r in ok)
        res = {
            "n": n, "c": c, "algo": self.algo, "seconds": round(wall, 3),
            "rps": round(n / wall, 1) if wall else None,
            "ok": len(ok), "failed": n - len(ok),
            "distribution": {b["name"]: dist.get(b["name"], 0) for b in BACKENDS},
            "latency_ms": None if not lat else {
                "min": round(lat[0], 1), "avg": round(statistics.mean(lat), 1),
                "p95": round(lat[max(0, math.ceil(0.95 * len(lat)) - 1)], 1), "max": round(lat[-1], 1)},
            "time": now_str(),
        }
        with self.lock:
            self.last_burst = res
        return res

    # ---------------------------------------------------------------- state for the page
    def lb_stats(self):
        try:
            with socket.create_connection((LB_HOST, LB_ADMIN), timeout=0.5) as s:
                s.sendall(b"GET / HTTP/1.0\r\n\r\n")
                data = b""
                while True:
                    chunk = s.recv(8192)
                    if not chunk:
                        break
                    data += chunk
            return json.loads(data.split(b"\r\n\r\n", 1)[1])
        except (OSError, ValueError, IndexError):
            return None

    def state(self):
        stats = self.lb_stats()
        by_port = {}
        if stats:
            for b in stats["backends"]:
                by_port[int(b["backend"].rsplit(":", 1)[1])] = b
        backends = []
        for b in BACKENDS:
            p = self.procs.get(b["port"])
            s = by_port.get(b["port"], {})
            backends.append({
                "name": b["name"], "port": b["port"], "delay": b["delay"],
                "process": bool(p and p.poll() is None),
                "state": s.get("state", "UNKNOWN"),
                "active": s.get("active_conns", 0), "total": s.get("total_conns", 0),
                "failed": s.get("failed_connects", 0),
                "bytes_in": s.get("bytes_client_to_backend", 0),
                "bytes_out": s.get("bytes_backend_to_client", 0),
            })
        now = int(time.time())
        with self.lock:
            series = []
            for sec in range(now - 60, now):       # the current second is still filling, so leave it out
                bucket = self.timeline.get(sec, {})
                row = {"t": sec, "fail": bucket.get("fail", 0)}
                for b in BACKENDS:
                    row[b["name"]] = bucket.get(b["name"], 0)
                series.append(row)
            recent_lat = [ms for sec in range(now - 9, now + 1) for ms in self.timeline.get(sec, {}).get("lat", [])]
            recent_n = sum(sum(v for k, v in self.timeline.get(sec, {}).items() if k.startswith("web-"))
                           for sec in range(now - 9, now))
            return {
                "lb": {"up": stats is not None, "algo": self.algo, "vip": f"{LB_HOST}:{LB_PORT}",
                       "interval": self.interval, "uptime_s": stats["uptime_s"] if stats else 0},
                "backends": backends,
                "traffic": {"running": self.traffic_running, "rate": self.traffic_rate,
                            "ok": self.ok, "fail": self.fail,
                            "availability": round(100 * self.ok / (self.ok + self.fail), 2) if self.ok + self.fail else None,
                            "rps_10s": round(recent_n / 9, 1),
                            "avg_latency_10s": round(statistics.mean(recent_lat), 1) if recent_lat else None},
                "series": series,
                "events": list(self.events)[:40],
                "connections": list(self.conns),
                "burst": self.last_burst,
                "clients": [{"ip": ip, "backend": be} for ip, be in sorted(self.client_map.items())],
                "hash_multi_ip": self.hash_ips_ok,
            }

    def reset(self):
        with self.lock:
            self.timeline.clear()
            self.ok = self.fail = 0
            self.last_burst = None
            self.client_map.clear()
            self.conns.clear()


DEMO = None


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):   # keep the terminal quiet
        pass

    def _send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if not n:
            return {}
        try:
            return json.loads(self.rfile.read(n))
        except ValueError:
            return {}

    def do_GET(self):
        if self.path in ("/", "/index.html", "/dashboard.html"):
            with open(os.path.join(HERE, "dashboard.html"), "rb") as f:
                self._send(200, f.read(), "text/html; charset=utf-8")
        elif self.path == "/api/state":
            self._send(200, DEMO.state())
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        body = self._body()
        m = re.fullmatch(r"/api/backend/(\d+)/(stop|start)", self.path)
        if m:
            port = int(m[1])
            if port not in [b["port"] for b in BACKENDS]:
                return self._send(404, {"error": "unknown backend"})
            DEMO.stop_backend(port) if m[2] == "stop" else DEMO.start_backend(port)
            return self._send(200, {"ok": True})
        if self.path == "/api/algo":
            algo = body.get("algo")
            if algo not in ("rr", "least", "hash"):
                return self._send(400, {"error": "algo must be rr, least or hash"})
            was = DEMO.traffic_running
            DEMO.traffic_running = False
            DEMO.algo = algo
            DEMO.start_lb()
            DEMO.reset()
            DEMO.event("info", f"Load balancer restarted with algorithm '{algo}' (counters reset)")
            DEMO.traffic_running = was
            return self._send(200, {"ok": True})
        if self.path == "/api/traffic":
            if "rate" in body:
                DEMO.traffic_rate = max(0.5, min(float(body["rate"]), 50.0))
            if "running" in body:
                DEMO.traffic_running = bool(body["running"])
            return self._send(200, {"ok": True})
        if self.path == "/api/burst":
            return self._send(200, DEMO.burst(body.get("n", 60), body.get("c", 6)))
        if self.path == "/api/reset":
            DEMO.reset()
            return self._send(200, {"ok": True})
        self._send(404, {"error": "not found"})


def port_in_use(port):
    try:
        socket.create_connection(("127.0.0.1", port), timeout=0.3).close()
        return True
    except OSError:
        return False


def main():
    global DEMO
    ap = argparse.ArgumentParser(description="Dashboard + launcher for the TCP load balancer demo")
    ap.add_argument("--port", type=int, default=5000, help="dashboard port (default 5000)")
    ap.add_argument("--interval", type=float, default=1.0, help="LB health-check interval in seconds")
    ap.add_argument("--no-browser", action="store_true", help="don't open the dashboard in a browser")
    args = ap.parse_args()

    busy = [p for p in [args.port, LB_PORT, LB_ADMIN] + [b["port"] for b in BACKENDS] if port_in_use(p)]
    if busy:
        print(f"Port(s) {busy} are already in use. Close any backend_server.py / load_balancer.py /")
        print("dashboard.py windows you started earlier, then run this again.")
        sys.exit(1)

    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)   # bind first, then start children
    srv.daemon_threads = True

    DEMO = Demo(args.interval)
    atexit.register(DEMO.shutdown)                                  # never leave orphan processes
    signal.signal(signal.SIGTERM, lambda *a: (_ for _ in ()).throw(KeyboardInterrupt()))
    for b in BACKENDS:
        DEMO.start_backend(b["port"])
    time.sleep(0.5)
    DEMO.start_lb()
    DEMO.event("info", "Demo started: 3 backends + load balancer on 127.0.0.1:8080")
    threading.Thread(target=DEMO.traffic_loop, daemon=True).start()

    print(f"Dashboard:  http://127.0.0.1:{args.port}   (Ctrl+C to stop everything)")
    print(f"Load balancer VIP {LB_HOST}:{LB_PORT}, stats {LB_HOST}:{LB_ADMIN}")
    if not args.no_browser:
        threading.Timer(0.8, webbrowser.open, args=(f"http://127.0.0.1:{args.port}",)).start()
    try:
        srv.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\nStopping all processes...")
    finally:
        DEMO.shutdown()
        srv.server_close()


if __name__ == "__main__":
    main()
