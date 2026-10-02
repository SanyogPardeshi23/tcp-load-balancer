#!/usr/bin/env python3
"""
load_balancer.py  -  a Layer-4 (TCP) load balancer with active + passive health checking.

What it does
------------
* Listens on one "virtual" address (default 127.0.0.1:8080) - the only address clients know.
* For every incoming TCP connection it picks a healthy backend using one of three algorithms:
      rr     round robin                (1, 2, 3, 1, 2, 3 ...)
      least  least active connections   (best when some requests are slow)
      hash   source-IP hash             ("sticky sessions": same client -> same server)
* Opens a SECOND, separate TCP connection to that backend and relays bytes both ways.
  (In Wireshark you therefore see two independent handshakes per request.)
* ACTIVE health check: every --interval seconds it tries to open a TCP connection
  (or send GET /health with --check http) to every backend.
  A backend is marked DOWN after --fall consecutive failures and UP again after
  --rise consecutive successes (hysteresis, so one lost packet does not flap it).
* PASSIVE health check / retry: if connecting to the chosen backend fails
  (e.g. it crashed between two health checks) the client is transparently retried
  on the next healthy backend, so the client never sees the failure.
* Admin/stats endpoint: http://127.0.0.1:8081/  returns live JSON statistics.

This is exactly what HAProxy / NGINX "stream" / AWS Network Load Balancer do,
reduced to ~300 lines of socket code.

Usage
-----
    python load_balancer.py --algo rr
    python load_balancer.py --algo least --check http --interval 1
    python load_balancer.py --backends 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003
"""

import argparse
import json
import socket
import threading
import time
import zlib
from datetime import datetime

BUF = 4096


def ts():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


_print_lock = threading.Lock()


def log(msg):
    with _print_lock:                      # keep lines from different threads from interleaving
        print(f"[{ts()}] {msg}", flush=True)


# --------------------------------------------------------------------------------------
# Backend bookkeeping
# --------------------------------------------------------------------------------------
class Backend:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.healthy = True          # optimistic start; the health checker corrects it within 1 interval
        self.active = 0              # connections currently being relayed
        self.total = 0               # connections ever sent here
        self.failed = 0              # connection attempts that failed
        self.bytes_to = 0            # client -> backend bytes
        self.bytes_from = 0          # backend -> client bytes
        self.fail_streak = 0         # consecutive failed health checks
        self.ok_streak = 0           # consecutive successful health checks
        self.lock = threading.Lock()

    @property
    def name(self):
        return f"{self.host}:{self.port}"

    def to_dict(self):
        return {
            "backend": self.name, "state": "UP" if self.healthy else "DOWN",
            "active_conns": self.active, "total_conns": self.total,
            "failed_connects": self.failed,
            "bytes_client_to_backend": self.bytes_to,
            "bytes_backend_to_client": self.bytes_from,
        }


class Pool:
    """The set of backends plus the scheduling algorithm."""

    def __init__(self, backends, algo):
        self.backends = backends
        self.algo = algo
        self.rr_index = 0
        self.lock = threading.Lock()

    def healthy(self, exclude=()):
        return [b for b in self.backends if b.healthy and b not in exclude]

    def choose(self, client_ip, exclude=()):
        """Return the backend for this client, or None if nothing is healthy."""
        with self.lock:
            candidates = self.healthy(exclude)
            if not candidates:
                return None
            if self.algo == "rr":
                # Walk the FULL list so the rotation order stays stable when a server drops out.
                n = len(self.backends)
                for _ in range(n):
                    b = self.backends[self.rr_index % n]
                    self.rr_index += 1
                    if b in candidates:
                        return b
                return candidates[0]
            if self.algo == "least":
                # Fewest in-flight connections; ties broken by fewest total.
                return min(candidates, key=lambda b: (b.active, b.total))
            if self.algo == "hash":
                # CRC32 of the client IP, modulo the number of healthy servers.
                # Same client -> same server, until the healthy set changes.
                idx = zlib.crc32(client_ip.encode()) % len(candidates)
                return candidates[idx]
            raise ValueError(self.algo)


# --------------------------------------------------------------------------------------
# Active health checking
# --------------------------------------------------------------------------------------
def probe(backend, mode, timeout):
    """One health probe. Returns True if the backend looks alive."""
    try:
        with socket.create_connection((backend.host, backend.port), timeout=timeout) as s:
            if mode == "tcp":
                return True                     # 3-way handshake completed -> port is open
            s.settimeout(timeout)
            s.sendall(b"GET /health HTTP/1.0\r\nHost: lb\r\n\r\n")
            reply = s.recv(64)
            return reply.startswith(b"HTTP/1.") and b" 200 " in reply
    except OSError:
        # ConnectionRefusedError = the OS answered our SYN with an RST (nothing listening)
        # socket.timeout          = no answer at all (host down / filtered)
        return False


def health_checker(pool, mode, interval, fall, rise, timeout):
    while True:
        for b in pool.backends:
            ok = probe(b, mode, timeout)
            with b.lock:
                if ok:
                    b.ok_streak += 1
                    b.fail_streak = 0
                    if not b.healthy and b.ok_streak >= rise:
                        b.healthy = True
                        log(f"HEALTH  {b.name} is UP again ({rise} good checks in a row)")
                else:
                    b.fail_streak += 1
                    b.ok_streak = 0
                    if b.healthy and b.fail_streak >= fall:
                        b.healthy = False
                        log(f"HEALTH  {b.name} marked DOWN ({fall} failed checks in a row)")
        time.sleep(interval)


# --------------------------------------------------------------------------------------
# Relaying one client connection
# --------------------------------------------------------------------------------------
def pump(src, dst, backend, direction, counter):
    """Copy bytes src -> dst until src closes, then half-close dst (propagate the FIN)."""
    try:
        while True:
            data = src.recv(BUF)
            if not data:
                break
            dst.sendall(data)
            counter[0] += len(data)
    except OSError:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)   # tell the other side "no more data" (TCP FIN)
        except OSError:
            pass
    with backend.lock:
        if direction == "up":
            backend.bytes_to += counter[0]
        else:
            backend.bytes_from += counter[0]


def handle_client(client, caddr, pool, connect_timeout, max_tries):
    t0 = time.perf_counter()
    tried = []
    upstream = backend = None

    # Pick a backend; if connecting fails, mark it and try the next one (passive health check).
    for _ in range(max_tries):
        backend = pool.choose(caddr[0], exclude=tried)
        if backend is None:
            break
        try:
            upstream = socket.create_connection((backend.host, backend.port), timeout=connect_timeout)
            upstream.settimeout(None)
            break
        except OSError as e:
            tried.append(backend)
            with backend.lock:
                backend.failed += 1
                backend.healthy = False          # take it out immediately, don't wait for the checker
                backend.ok_streak = 0
            log(f"RETRY   {caddr[0]}:{caddr[1]} -> {backend.name} failed ({e.__class__.__name__}); "
                f"marked DOWN, trying next backend")
            upstream = backend = None

    if upstream is None:
        log(f"DROP    {caddr[0]}:{caddr[1]} - no healthy backend available")
        client.close()
        return

    with backend.lock:
        backend.active += 1
        backend.total += 1

    up, down = [0], [0]
    t_up = threading.Thread(target=pump, args=(client, upstream, backend, "up", up), daemon=True)
    t_down = threading.Thread(target=pump, args=(upstream, client, backend, "down", down), daemon=True)
    t_up.start(); t_down.start()
    t_up.join(); t_down.join()
    client.close(); upstream.close()

    with backend.lock:
        backend.active -= 1
    ms = (time.perf_counter() - t0) * 1000
    log(f"CONN    {caddr[0]}:{caddr[1]} -> {backend.name}  [{pool.algo}]  "
        f"up {up[0]} B / down {down[0]} B  {ms:.1f} ms")


# --------------------------------------------------------------------------------------
# Admin / stats endpoint (plain HTTP, JSON body)
# --------------------------------------------------------------------------------------
def admin_server(host, port, pool, started):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(16)
    while True:
        conn, _ = srv.accept()
        try:
            conn.settimeout(2)
            conn.recv(1024)
            body = json.dumps({
                "algorithm": pool.algo,
                "uptime_s": round(time.time() - started, 1),
                "backends": [b.to_dict() for b in pool.backends],
            }, indent=2).encode()
            conn.sendall(b"HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n"
                         b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
        except OSError:
            pass
        finally:
            conn.close()


def status_printer(pool, every):
    while True:
        time.sleep(every)
        rows = [f"  {b.name:<17} {'UP' if b.healthy else 'DOWN':<5} active={b.active:<3} "
                f"total={b.total:<5} failed={b.failed}" for b in pool.backends]
        log("STATUS\n" + "\n".join(rows))


# --------------------------------------------------------------------------------------
def parse_backends(text):
    out = []
    for item in text.split(","):
        host, port = item.strip().rsplit(":", 1)
        out.append(Backend(host, int(port)))
    return out


def main():
    p = argparse.ArgumentParser(description="Layer-4 TCP load balancer with health checks")
    p.add_argument("--listen", default="127.0.0.1:8080", help="virtual IP:port clients connect to")
    p.add_argument("--backends", default="127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003")
    p.add_argument("--algo", choices=["rr", "least", "hash"], default="rr")
    p.add_argument("--check", choices=["tcp", "http"], default="tcp",
                   help="tcp = handshake only (L4); http = GET /health must return 200 (L7)")
    p.add_argument("--interval", type=float, default=2.0, help="seconds between health-check rounds")
    p.add_argument("--fall", type=int, default=2, help="failed checks before marking DOWN")
    p.add_argument("--rise", type=int, default=2, help="good checks before marking UP")
    p.add_argument("--timeout", type=float, default=1.0, help="connect/probe timeout (s)")
    p.add_argument("--admin", default="127.0.0.1:8081", help="stats endpoint IP:port")
    p.add_argument("--status-every", type=float, default=0, help="print a status table every N s (0=off)")
    args = p.parse_args()

    pool = Pool(parse_backends(args.backends), args.algo)
    started = time.time()

    threading.Thread(target=health_checker, daemon=True,
                     args=(pool, args.check, args.interval, args.fall, args.rise, args.timeout)).start()
    ahost, aport = args.admin.rsplit(":", 1)
    threading.Thread(target=admin_server, args=(ahost, int(aport), pool, started), daemon=True).start()
    if args.status_every > 0:
        threading.Thread(target=status_printer, args=(pool, args.status_every), daemon=True).start()

    lhost, lport = args.listen.rsplit(":", 1)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((lhost, int(lport)))
    srv.listen(256)
    srv.settimeout(1.0)   # so Ctrl+C is noticed on Windows

    log(f"Load balancer on {args.listen}  algo={args.algo}  check={args.check} "
        f"every {args.interval}s (fall={args.fall}, rise={args.rise})")
    log(f"Backends: {', '.join(b.name for b in pool.backends)}")
    log(f"Stats:    http://{args.admin}/")
    try:
        while True:
            try:
                client, caddr = srv.accept()
            except socket.timeout:
                continue
            threading.Thread(target=handle_client,
                             args=(client, caddr, pool, args.timeout, len(pool.backends)),
                             daemon=True).start()
    except KeyboardInterrupt:
        log("Shutting down.")
    finally:
        srv.close()


if __name__ == "__main__":
    main()
