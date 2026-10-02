#!/usr/bin/env python3
"""
client_loadgen.py  -  traffic generator + measurement tool for the load balancer.

Two modes
---------
1. Burst (default): send N requests with C concurrent workers, then print
   how requests were distributed across backends, the success rate and latency stats.

       python client_loadgen.py -n 30 -c 5

2. Continuous (--watch): one request every --gap seconds, printed live, until Ctrl+C.
   Use this during the failover demo: kill a backend and watch traffic move away
   from it with zero (or near-zero) failed requests.

       python client_loadgen.py --watch --gap 0.3

Each request is a brand-new TCP connection (HTTP/1.0, "Connection: close"),
exactly like the backends expect.
"""

import argparse
import math
import socket
import statistics
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


def ts():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


SOURCE_IP = None   # set by --source-ip; lets one PC pretend to be several clients


def one_request(host, port, path="/", timeout=5.0, source_ip=None):
    """Returns (ok, backend_name_or_error, latency_ms)."""
    t0 = time.perf_counter()
    try:
        ip = source_ip or SOURCE_IP
        src = (ip, 0) if ip else None          # port 0 = let the OS pick an ephemeral port
        with socket.create_connection((host, port), timeout=timeout,
                                      source_address=src) as s:   # SYN / SYN-ACK / ACK
            s.sendall(f"GET {path} HTTP/1.0\r\nHost: {host}\r\n\r\n".encode())
            chunks = []
            while True:
                data = s.recv(4096)
                if not data:          # server sent FIN
                    break
                chunks.append(data)
        ms = (time.perf_counter() - t0) * 1000
        reply = b"".join(chunks).decode(errors="replace")
        if not reply.startswith("HTTP/1.0 200"):
            return False, "empty/invalid reply", ms
        body = reply.split("\r\n\r\n", 1)[1]
        # body looks like: "Served by web-2 (port 9002) | request #7 | ..."
        name = body.split("Served by ", 1)[1].split(" ", 1)[0] if "Served by " in body else "?"
        return True, name, ms
    except OSError as e:
        return False, e.__class__.__name__, (time.perf_counter() - t0) * 1000


def burst(host, port, n, c):
    results = []
    lock = threading.Lock()

    def work(_):
        r = one_request(host, port)
        with lock:
            results.append(r)

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=c) as ex:
        list(ex.map(work, range(n)))
    wall = time.perf_counter() - t0

    ok = [r for r in results if r[0]]
    bad = [r for r in results if not r[0]]
    dist = Counter(r[1] for r in ok)
    lat = sorted(r[2] for r in ok)

    print(f"\nSent {n} requests, concurrency {c}, in {wall:.2f} s  "
          f"-> {n / wall:.1f} req/s")
    print(f"Success: {len(ok)}   Failed: {len(bad)}")
    if bad:
        print("  failure reasons:", dict(Counter(r[1] for r in bad)))
    print("\nDistribution across backends:")
    for name, cnt in sorted(dist.items()):
        bar = "#" * cnt
        print(f"  {name:<8} {cnt:>4}  ({100 * cnt / max(1, len(ok)):5.1f}%)  {bar}")
    if lat:
        p95 = lat[max(0, math.ceil(0.95 * len(lat)) - 1)]   # nearest-rank percentile
        print(f"\nLatency ms: min {lat[0]:.1f}  avg {statistics.mean(lat):.1f}  "
              f"p95 {p95:.1f}  max {lat[-1]:.1f}")


def watch(host, port, gap):
    ok_count = fail_count = 0
    print("Continuous mode - Ctrl+C to stop.\n")
    try:
        while True:
            ok, who, ms = one_request(host, port)
            if ok:
                ok_count += 1
                print(f"[{ts()}] OK    {who:<8} {ms:6.1f} ms")
            else:
                fail_count += 1
                print(f"[{ts()}] FAIL  {who:<8} {ms:6.1f} ms   <-----")
            time.sleep(gap)
    except KeyboardInterrupt:
        total = ok_count + fail_count
        print(f"\n{total} requests: {ok_count} ok, {fail_count} failed "
              f"({100 * ok_count / max(1, total):.2f}% availability)")


def main():
    p = argparse.ArgumentParser(description="Load generator for the TCP load balancer")
    p.add_argument("--target", default="127.0.0.1:8080")
    p.add_argument("-n", type=int, default=30, help="number of requests (burst mode)")
    p.add_argument("-c", type=int, default=5, help="concurrent workers (burst mode)")
    p.add_argument("--watch", action="store_true", help="continuous mode for the failover demo")
    p.add_argument("--gap", type=float, default=0.3, help="seconds between requests in --watch")
    p.add_argument("--source-ip", help="bind to this local IP, e.g. 127.0.0.2 (tests --algo hash)")
    args = p.parse_args()
    global SOURCE_IP
    SOURCE_IP = args.source_ip
    host, port = args.target.rsplit(":", 1)
    if args.watch:
        watch(host, int(port), args.gap)
    else:
        burst(host, int(port), args.n, args.c)


if __name__ == "__main__":
    main()
