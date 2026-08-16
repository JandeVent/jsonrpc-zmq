#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Baseline benchmark for the Qt JSON-RPC peer (jsonrpc_zmq.qt_peer).

Measures:
  - idle CPU cost per peer (1 peer and 30 peers)
  - request throughput, main thread (rps)
  - request throughput, worker thread (rps)
  - round-trip latency percentiles (p50 / p95 / p99)

Every run writes a machine-dated JSON record to bench/results/
(e.g. bench/results/2026-08-16_myhost.json) so later runs can be
compared with:

    python3 bench/bench_qt_peer.py --compare

Usage:
    python3 bench/bench_qt_peer.py            # run all benchmarks
    python3 bench/bench_qt_peer.py --idle     # idle CPU only
    python3 bench/bench_qt_peer.py --compare  # show recorded runs side by side
"""

import argparse
import datetime
import json
import os
import platform
import resource
import statistics
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qtpy.QtWidgets import QApplication
from jsonrpcserver import Success

from jsonrpc_zmq import QJsonRpcPeer

BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BENCH_DIR, "results")

# port range used by the benchmarks (loopback, bind on 127.0.0.1)
_PORT = [5920]


def _next_addr():
    _PORT[0] += 1
    return f"tcp://127.0.0.1:{_PORT[0]}"


def _cpu_seconds():
    r = resource.getrusage(resource.RUSAGE_SELF)
    return r.ru_utime + r.ru_stime


def _pump(seconds):
    """Pump the Qt event loop for the given duration."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < seconds:
        QApplication.processEvents()
        time.sleep(0.002)


def _make_pair(addr):
    server = QJsonRpcPeer(addr, bind=True)
    server.add_handler("ping", lambda: Success("pong"))
    server.start()
    client = QJsonRpcPeer(addr, bind=False)
    client.start()
    return server, client


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

def bench_idle_cpu(num_peers: int, seconds: float = 10.0) -> dict:
    """CPU cost of N idle peers (no traffic at all)."""
    peers = [QJsonRpcPeer(_next_addr(), bind=True) for _ in range(num_peers)]
    for p in peers:
        p.start()
    try:
        base = _cpu_seconds()
        t0 = time.monotonic()
        _pump(seconds)
        cpu = _cpu_seconds() - base
        elapsed = time.monotonic() - t0
    finally:
        for p in peers:
            p.stop()
    return {
        "peers": num_peers,
        "seconds": round(elapsed, 3),
        "cpu_seconds": round(cpu, 4),
        "cpu_percent": round(cpu / elapsed * 100, 2),
        "cpu_percent_per_peer": round(cpu / elapsed * 100 / num_peers, 3),
    }


def bench_throughput_main(seconds: float = 5.0) -> dict:
    """Sequential request() calls driven from the main thread."""
    server, client = _make_pair(_next_addr())
    try:
        n = 0
        t0 = time.monotonic()
        while time.monotonic() - t0 < seconds:
            client.request("ping", timeout=5)
            n += 1
        elapsed = time.monotonic() - t0
    finally:
        client.stop()
        server.stop()
    return {"seconds": round(elapsed, 3), "requests": n,
            "rps": round(n / elapsed, 1)}


def bench_throughput_worker(seconds: float = 5.0) -> dict:
    """request() calls from a plain worker thread (main thread pumping)."""
    server, client = _make_pair(_next_addr())
    stop = [False]
    ok = [0]
    errors = []

    def hammer():
        while not stop[0]:
            try:
                client.request("ping", timeout=5)
                ok[0] += 1
            except Exception as e:  # pragma: no cover
                errors.append(repr(e))
                stop[0] = True

    th = threading.Thread(target=hammer, daemon=True)
    th.start()
    try:
        t0 = time.monotonic()
        _pump(seconds)
        elapsed = time.monotonic() - t0
        stop[0] = True
        # keep pumping briefly so an in-flight request at the boundary
        # drains instead of timing out (measurement artifact)
        _pump(1.0)
        th.join(timeout=5)
    finally:
        client.stop()
        server.stop()
    return {"seconds": round(elapsed, 3), "requests": ok[0],
            "rps": round(ok[0] / elapsed, 1), "errors": len(errors)}


def bench_latency(n: int = 500) -> dict:
    """Round-trip latency percentiles for n sequential requests."""
    server, client = _make_pair(_next_addr())
    samples = []
    try:
        for _ in range(n):
            t0 = time.perf_counter()
            client.request("ping", timeout=5)
            samples.append((time.perf_counter() - t0) * 1000)  # ms
    finally:
        client.stop()
        server.stop()
    samples.sort()
    p = lambda q: round(samples[int(q * (len(samples) - 1))], 3)
    return {
        "n": n,
        "p50_ms": p(0.50),
        "p95_ms": p(0.95),
        "p99_ms": p(0.99),
        "min_ms": round(samples[0], 3),
        "max_ms": round(samples[-1], 3),
        "mean_ms": round(statistics.mean(samples), 3),
    }


# ---------------------------------------------------------------------------
# Environment / record helpers
# ---------------------------------------------------------------------------

def collect_environment() -> dict:
    try:
        import qtpy
        import zmq
        import jsonrpcserver
        import jsonrpcclient
    except Exception:  # pragma: no cover
        pass
    return {
        "hostname": platform.node(),
        "machine": platform.machine(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "python": platform.python_version(),
        "qt_api": qtpy.API_NAME,
        "qt_version": qtpy.QT_VERSION,
        "pyzmq": zmq.__version__,
        "libzmq": zmq.zmq_version(),
        "jsonrpcserver": getattr(jsonrpcserver, "__version__", "?"),
        "jsonrpcclient": getattr(jsonrpcclient, "__version__", "?"),
    }


def run_all() -> dict:
    record = {
        "date": datetime.datetime.now().isoformat(timespec="seconds"),
        "machine": collect_environment(),
        "results": {},
    }
    print("== idle CPU ==")
    for peers in (1, 30):
        r = bench_idle_cpu(peers)
        record["results"][f"idle_cpu_{peers}"] = r
        print(f"  {peers} idle peers: {r['cpu_percent']}% CPU "
              f"({r['cpu_percent_per_peer']}%/peer)")

    print("== throughput ==")
    r = bench_throughput_main()
    record["results"]["throughput_main"] = r
    print(f"  main thread:   {r['rps']} rps")
    r = bench_throughput_worker()
    record["results"]["throughput_worker"] = r
    print(f"  worker thread: {r['rps']} rps")

    print("== latency ==")
    r = bench_latency()
    record["results"]["latency"] = r
    print(f"  p50={r['p50_ms']}ms p95={r['p95_ms']}ms p99={r['p99_ms']}ms "
          f"max={r['max_ms']}ms (n={r['n']})")
    return record


def save_record(record: dict) -> str:
    os.makedirs(RESULTS_DIR, exist_ok=True)
    ts = datetime.datetime.fromisoformat(record["date"]).strftime("%Y-%m-%d_%H%M%S")
    path = os.path.join(
        RESULTS_DIR,
        f"{ts}_{record['machine']['hostname']}.json",
    )
    with open(path, "w") as f:
        json.dump(record, f, indent=2)
    return path


def compare_records() -> None:
    if not os.path.isdir(RESULTS_DIR):
        print("no records yet")
        return
    rows = []
    for name in sorted(os.listdir(RESULTS_DIR)):
        if not name.endswith(".json"):
            continue
        with open(os.path.join(RESULTS_DIR, name)) as f:
            rec = json.load(f)
        m, res = rec["machine"], rec["results"]
        rows.append({
            "file": name,
            "date": rec["date"],
            "host": m["hostname"],
            "qt": f"{m['qt_api']} {m['qt_version']}",
            "idle1_cpu%": res.get("idle_cpu_1", {}).get("cpu_percent"),
            "idle30_cpu%": res.get("idle_cpu_30", {}).get("cpu_percent"),
            "main_rps": res.get("throughput_main", {}).get("rps"),
            "worker_rps": res.get("throughput_worker", {}).get("rps"),
            "p50_ms": res.get("latency", {}).get("p50_ms"),
            "p99_ms": res.get("latency", {}).get("p99_ms"),
        })
    if not rows:
        print("no records yet")
        return
    headers = list(rows[0].keys())
    widths = {h: max(len(h), *(len(str(r[h])) for r in rows)) for h in headers}
    print(" ".join(h.ljust(widths[h]) for h in headers))
    for r in rows:
        print(" ".join(str(r[h]).ljust(widths[h]) for h in headers))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--idle", action="store_true", help="run idle CPU benchmarks only")
    ap.add_argument("--compare", action="store_true",
                    help="print recorded runs side by side and exit")
    args = ap.parse_args()

    if args.compare:
        compare_records()
        return

    app = QApplication.instance() or QApplication([])

    if args.idle:
        record = {
            "date": datetime.datetime.now().isoformat(timespec="seconds"),
            "machine": collect_environment(),
            "results": {},
        }
        for peers in (1, 30):
            r = bench_idle_cpu(peers)
            record["results"][f"idle_cpu_{peers}"] = r
            print(f"  {peers} idle peers: {r['cpu_percent']}% CPU "
                  f"({r['cpu_percent_per_peer']}%/peer)")
    else:
        record = run_all()

    path = save_record(record)
    print(f"\nrecord saved: {path}")


if __name__ == "__main__":
    main()
