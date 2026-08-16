# -*- coding: utf-8 -*-
# Copyright (C) 2018-2026 Connet Information Technology Company, Shanghai.

"""Qt JSON-RPC peer tests.

Harness rules (the old suite hung in endless waits):
- every peer is stopped before the test ends — a running io QThread at
  teardown blocks the interpreter on destruction;
- every wait is bounded (qtbot.waitSignal / waitUntil / request timeout);
- no custom QApplication fixture — pytest-qt owns the app.
"""

import threading
import time

import pytest
from jsonrpcserver import Success

from jsonrpc_zmq.exceptions import JsonRpcError, RequestTimeoutError, TransportError
from jsonrpc_zmq.qt_peer import QJsonRpcPeer

# port counter: a fresh loopback address per peer, avoiding TIME_WAIT clashes
_port = [5560]


def _next_addr() -> str:
    _port[0] += 1
    return f"tcp://127.0.0.1:{_port[0]}"


@pytest.fixture
def peer(qtbot):
    """A single started peer, always stopped on teardown."""
    p = QJsonRpcPeer(_next_addr(), bind=True)
    p.start()
    yield p
    p.stop()


@pytest.fixture
def peer_pair(qtbot):
    """A bound server + connected client, both started and always stopped."""
    addr = _next_addr()
    server = QJsonRpcPeer(addr, bind=True)
    client = QJsonRpcPeer(addr, bind=False)
    server.start()
    client.start()
    yield server, client
    client.stop()
    server.stop()


# =============================================================================
# Lifecycle / state errors
# =============================================================================

def test_request_before_start_times_out(qtbot):
    """request() on an unstarted peer times out instead of hanging."""
    p = QJsonRpcPeer(_next_addr(), bind=False)
    try:
        with pytest.raises(RequestTimeoutError):
            p.request("ping", timeout=0.5)
    finally:
        p.stop()


def test_request_after_stop_times_out(qtbot):
    """request() on a stopped peer times out (send fails, no response)."""
    p = QJsonRpcPeer(_next_addr(), bind=False)
    p.start()
    p.stop()
    try:
        with pytest.raises(RequestTimeoutError):
            p.request("ping", timeout=0.5)
    finally:
        p.stop()  # idempotent — must not raise


def test_stop_is_idempotent(qtbot, peer):
    """stop() twice is a no-op, and the peer reports stopped."""
    peer.stop()
    peer.stop()
    assert peer.stopped()


# =============================================================================
# Request / response
# =============================================================================

def test_request_roundtrip(qtbot, peer_pair):
    """A handler result travels back to the requester."""
    server, client = peer_pair
    server.add_handler("add", lambda a, b: Success(a + b))
    assert client.request("add", {"a": 2, "b": 3}, timeout=5) == 5


def test_handler_exception_becomes_jsonrpc_error(qtbot, peer_pair):
    """An exception inside a handler surfaces as a JsonRpcError."""
    server, client = peer_pair

    def boom():
        raise RuntimeError("boom")

    server.add_handler("boom", boom)
    with pytest.raises(JsonRpcError):
        client.request("boom", timeout=5)


def test_handler_returning_none_yields_error(qtbot, peer_pair):
    """A handler returning None fails server-side validation and the
    client receives a JsonRpcError instead of hanging."""
    server, client = peer_pair
    server.add_handler("noop", lambda: None)
    with pytest.raises(JsonRpcError):
        client.request("noop", timeout=5)


def test_request_timeout_and_pending_cleanup(qtbot, peer_pair):
    """A slow handler + short timeout raises RequestTimeoutError and
    leaves no pending entry behind."""
    server, client = peer_pair

    def slow():
        time.sleep(0.2)
        return Success("ok")

    server.add_handler("slow", slow)
    with pytest.raises(RequestTimeoutError):
        client.request("slow", timeout=0.05)
    assert client._pending == {}


def test_notify_delivered(qtbot, peer_pair):
    """A notification reaches the peer's requestReceived signal."""
    server, client = peer_pair
    with qtbot.waitSignal(server._recv_worker.requestReceived, timeout=2000):
        client.notify("ping")


# =============================================================================
# Backpressure
# =============================================================================

def test_backpressure_queue_full(qtbot, peer_pair):
    """A full send queue surfaces as a TransportError through the
    server's error signal (triggered by a response it cannot queue)."""
    server, client = peer_pair
    server._send_worker._send_queue.extend(
        [b"x"] * server._send_worker._send_queue.maxlen
    )
    server.add_handler("echo", lambda: Success("pong"))

    with pytest.raises(RequestTimeoutError):
        with qtbot.waitSignal(server._send_worker.exceptionOccurred, timeout=2000) as catcher:
            client.request("echo", timeout=0.2)
    assert isinstance(catcher.args[0], TransportError)


# =============================================================================
# Transport errors stop the peer
# =============================================================================

def test_send_transport_error_triggers_stop(qtbot, peer):
    """A send failure surfaces and stops the peer."""
    def boom(msg):
        raise RuntimeError("send failed")

    peer._socket.send = boom
    with qtbot.waitSignal(peer._send_worker.exceptionOccurred, timeout=2000):
        peer.notify("test")
    qtbot.waitUntil(lambda: peer.stopped(), timeout=2000)


def test_recv_transport_error_triggers_stop(qtbot, peer):
    """A receive failure surfaces and stops the peer.

    Driven through the worker's error signal: forcing a real socket
    error would require closing the fd underneath the notifier, which
    the peer's shutdown already guards against.
    """
    with qtbot.waitSignal(peer._recv_worker.exceptionOccurred, timeout=2000):
        peer._recv_worker.exceptionOccurred.emit(TransportError("recv failed"))
    qtbot.waitUntil(lambda: peer.stopped(), timeout=2000)


# =============================================================================
# Event-driven transport (no polling): bursts, worker threads, backpressure
# =============================================================================

def test_sequential_burst_roundtrip(qtbot, peer_pair):
    """Hundreds of back-to-back requests all resolve (regression: lost
    cross-thread wakeups used to drop an occasional response)."""
    server, client = peer_pair
    server.add_handler("add", lambda a, b: Success(a + b))
    for i in range(300):
        assert client.request("add", {"a": i, "b": 1}, timeout=5) == i + 1


def test_worker_thread_roundtrip(qtbot, peer_pair):
    """request() from a plain threading.Thread resolves correctly
    (regression: the response path must not depend on the calling
    thread's Qt event loop)."""
    server, client = peer_pair
    server.add_handler("ping", lambda: Success("pong"))

    result = {}
    def caller():
        try:
            result["value"] = client.request("ping", timeout=5)
        except Exception as e:
            result["exc"] = repr(e)

    th = threading.Thread(target=caller, daemon=True)
    th.start()
    qtbot.waitUntil(lambda: "value" in result or "exc" in result, timeout=5000)
    th.join(timeout=5)
    assert result.get("exc") is None
    assert result.get("value") == "pong"


def test_concurrent_worker_threads(qtbot, peer_pair):
    """Multiple worker threads issuing requests concurrently."""
    server, client = peer_pair
    server.add_handler("ping", lambda: Success("pong"))

    n_threads, per_thread = 4, 25
    done = [0]
    errors = []
    lock = threading.Lock()

    def caller():
        try:
            for _ in range(per_thread):
                assert client.request("ping", timeout=5) == "pong"
                with lock:
                    done[0] += 1
        except Exception as e:
            with lock:
                errors.append(repr(e))

    threads = [threading.Thread(target=caller, daemon=True) for _ in range(n_threads)]
    for t in threads:
        t.start()
    qtbot.waitUntil(lambda: done[0] >= n_threads * per_thread or bool(errors), timeout=15000)
    for t in threads:
        t.join(timeout=5)
    assert errors == []
    assert done[0] == n_threads * per_thread


def test_stop_fails_in_flight_requests(qtbot, peer):
    """stop() fails pending requests with TransportError instead of
    leaving the caller blocked forever."""
    result = {}
    def caller():
        try:
            peer.request("ping", timeout=None)
            result["exc"] = None
        except Exception as e:
            result["exc"] = type(e).__name__

    th = threading.Thread(target=caller, daemon=True)
    th.start()
    qtbot.waitUntil(lambda: len(peer._pending) > 0, timeout=2000)
    peer.stop()
    th.join(timeout=5)
    assert result.get("exc") == "TransportError"


def test_send_backpressure_retry_delivers(qtbot, peer_pair):
    """A zmq.Again on the first send arms the retry timer and the
    message is delivered once the socket accepts it again."""
    import zmq

    server, client = peer_pair
    server.add_handler("ping", lambda: Success("pong"))
    real_send = client._socket.send
    state = {"again": True}

    def flaky_send(msg, flags=0):
        if state["again"]:
            state["again"] = False
            raise zmq.Again()
        return real_send(msg, flags=flags)

    client._socket.send = flaky_send
    assert client.request("ping", timeout=5) == "pong"
