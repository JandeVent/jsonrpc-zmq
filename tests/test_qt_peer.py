import pytest
import time
from qtpy.QtWidgets import QApplication
from jsonrpc_zmq.exceptions import RequestTimeoutError, TransportError, JsonRpcError
from jsonrpc_zmq.qt_peer import QJsonRpcPeer


@pytest.fixture(scope="module")
def app():
    """提供 QApplication"""
    app = QApplication.instance()
    if not app:
        app = QApplication([])
    return app


# -------------------------
# TEST 1: 状态机异常
# -------------------------
def test_invalid_state_errors(qtbot, app):
    addr = "tcp://127.0.0.1:5560"

    # 1. request / notify before start
    peer = QJsonRpcPeer(addr, bind=False)
    for method in [peer.request, peer.notify]:
        with pytest.raises(Exception):
            method("ping", timeout=1)
    peer.start()

    # 2. request after stop
    peer2 = QJsonRpcPeer(addr, bind=False)
    peer2.start()
    peer2.stop()
    for method in [peer2.request, peer2.notify]:
        with pytest.raises(Exception):
            method("ping", timeout=1)

    # 3. start twice
    peer3 = QJsonRpcPeer(addr, bind=False)
    peer3.start()
    with pytest.raises(Exception):
        peer3.start()
    peer3.stop()

    # 4. stop twice (should be no-op)
    peer4 = QJsonRpcPeer(addr, bind=False)
    peer4.start()
    peer4.stop()
    peer4.stop()


# -------------------------
# TEST 2: 背压测试
# -------------------------
def test_backpressure_queue_full(qtbot, app):
    addr = "tcp://127.0.0.1:5561"

    server = QJsonRpcPeer(addr, bind=True)
    server.start()

    # 强制 send_queue 达到 maxlen
    server._send_worker._send_queue = [b"x"] * server._send_worker._send_queue.maxlen

    client = QJsonRpcPeer(addr, bind=False)
    client.start()

    caught = []

    def on_exception(e):
        caught.append(e)

    server._send_worker.exceptionOccurred.connect(on_exception)
    client.notify("test")  # 触发满队列
    qtbot.waitUntil(lambda: len(caught) > 0, timeout=1000)

    assert any(isinstance(e, TransportError) for e in caught)

    server.stop()
    client.stop()


# -------------------------
# TEST 3: request timeout / pending cleanup
# -------------------------
def test_request_timeout_and_pending_cleanup(qtbot, app):
    addr = "tcp://127.0.0.1:5562"

    server = QJsonRpcPeer(addr, bind=True)

    # handler sleep 0.2s
    def slow():
        time.sleep(0.2)
        return "ok"

    server.add_handler("slow", slow)
    server.start()

    client = QJsonRpcPeer(addr, bind=False)
    client.start()

    # timeout = 0.05s
    with pytest.raises(RequestTimeoutError):
        client.request("slow", timeout=0.05)

    # pending 应该被清理
    assert client._pending == {}

    server.stop()
    client.stop()


# -------------------------
# TEST 4: handler 异常 / 返回 None
# -------------------------
def test_handler_exception_and_none(qtbot, app):
    addr = "tcp://127.0.0.1:5563"

    server = QJsonRpcPeer(addr, bind=True)

    # handler 抛异常
    def boom():
        raise RuntimeError("boom")

    server.add_handler("boom", boom)
    server.start()

    client = QJsonRpcPeer(addr, bind=False)
    client.start()

    with pytest.raises(JsonRpcError):
        client.request("boom", timeout=0.5)

    # handler 返回 None (相当于 noop)
    def noop():
        return None

    server.add_handler("noop", noop)
    # 不抛异常
    with pytest.raises(JsonRpcError):
        client.request("noop", timeout=0.5)

    server.stop()
    client.stop()


# -------------------------
# TEST 5: send / recv transport exception
# -------------------------
def test_send_transport_error_triggers_stop(qtbot, app):
    addr = "tcp://127.0.0.1:5564"
    peer = QJsonRpcPeer(addr, bind=False)
    peer.start()

    # monkey patch send 抛异常
    def boom(msg):
        raise RuntimeError("send failed")

    peer._socket.send = boom

    caught = []

    peer.exceptionOccurred.connect(lambda e: caught.append(e))
    peer.notify("test")
    qtbot.waitUntil(lambda: len(caught) > 0, timeout=500)
    assert any(isinstance(e, RuntimeError) for e in caught)
    assert peer.stopped()
    peer.stop()


def test_recv_transport_error_triggers_stop(qtbot, app):
    addr = "tcp://127.0.0.1:5565"
    peer = QJsonRpcPeer(addr, bind=False)
    peer.start()

    # monkey patch recv 抛异常
    def boom():
        raise RuntimeError("recv failed")

    peer._socket.recv = boom

    caught = []
    peer._recv_worker.exceptionOccurred.connect(lambda e: caught.append(e))
    qtbot.waitUntil(lambda: len(caught) > 0, timeout=500)

    assert any(isinstance(e, RuntimeError) for e in caught)
    assert peer.stopped()
    peer.stop()