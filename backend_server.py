#!/usr/bin/env python3
"""
backend_server.py  -  one "web server" in the backend pool.

A deliberately tiny HTTP/1.0 server written with raw TCP sockets (no http.server,
no frameworks) so that every byte on the wire is ours and easy to explain in Wireshark.

Every request is served on its own TCP connection and the server closes the
connection after replying (HTTP/1.0 behaviour). That means each request shows
the full TCP life-cycle in a capture: SYN -> SYN/ACK -> ACK -> data -> FIN.

Endpoints
    GET /health   -> "200 OK" with body "OK"      (used by the load balancer's health checker)
    GET /<other>  -> "200 OK" with a line saying which backend served it

Run three of these on different ports, e.g.
    python backend_server.py --port 9001 --name web-1
    python backend_server.py --port 9002 --name web-2
    python backend_server.py --port 9003 --name web-3 --delay 150     (a "slow" server)
"""

import argparse
import socket
import threading
import time
from datetime import datetime

request_counter = 0
counter_lock = threading.Lock()


def ts():
    """Timestamp for log lines, millisecond precision (useful to line up with Wireshark)."""
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def read_http_request(conn):
    """Read from the socket until the end of the HTTP headers (a blank line)."""
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(1024)
        if not chunk:          # peer closed before sending a full request
            break
        data += chunk
        if len(data) > 8192:   # refuse absurdly large headers
            break
    return data


def build_response(status, body):
    body_bytes = body.encode()
    header = (
        f"HTTP/1.0 {status}\r\n"
        f"Content-Type: text/plain\r\n"
        f"Content-Length: {len(body_bytes)}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    )
    return header.encode() + body_bytes


def handle_client(conn, addr, name, port, delay_ms):
    global request_counter
    try:
        raw = read_http_request(conn)
        if not raw:
            return
        request_line = raw.split(b"\r\n", 1)[0].decode(errors="replace")
        parts = request_line.split()
        path = parts[1] if len(parts) >= 2 else "/"

        if path == "/health":
            conn.sendall(build_response("200 OK", "OK\n"))
            # Health checks are frequent, so we do not log them (keeps the console readable).
            return

        with counter_lock:
            request_counter += 1
            n = request_counter

        if delay_ms:
            time.sleep(delay_ms / 1000.0)   # simulate a slower / busier server

        body = (f"Served by {name} (port {port}) | request #{n} | "
                f"TCP peer {addr[0]}:{addr[1]}\n")
        conn.sendall(build_response("200 OK", body))
        print(f"[{ts()}] {name}: #{n} {request_line!r} from {addr[0]}:{addr[1]}")
    except OSError as e:
        print(f"[{ts()}] {name}: error with {addr}: {e}")
    finally:
        conn.close()   # sends our FIN -> the connection closes cleanly


def main():
    p = argparse.ArgumentParser(description="Minimal backend web server (raw sockets)")
    p.add_argument("--host", default="127.0.0.1", help="address to bind (0.0.0.0 = all interfaces)")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--name", required=True, help="label printed in every response, e.g. web-1")
    p.add_argument("--delay", type=int, default=0, help="artificial processing delay in ms")
    args = p.parse_args()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)          # IPv4 + TCP
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)        # allow quick restart
    srv.bind((args.host, args.port))
    srv.listen(128)                                                  # backlog of pending connections
    srv.settimeout(1.0)   # wake up every second so Ctrl+C works on Windows too

    print(f"[{ts()}] {args.name} listening on {args.host}:{args.port} "
          f"(delay {args.delay} ms). Ctrl+C to stop.")
    try:
        while True:
            try:
                conn, addr = srv.accept()            # completes the 3-way handshake
            except socket.timeout:
                continue
            threading.Thread(target=handle_client,
                             args=(conn, addr, args.name, args.port, args.delay),
                             daemon=True).start()
    except KeyboardInterrupt:
        print(f"\n[{ts()}] {args.name} shutting down.")
    finally:
        srv.close()


if __name__ == "__main__":
    main()
