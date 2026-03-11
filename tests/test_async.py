# ================== 测试矩阵 ==============================
#
# ① 状态机异常
# TEST 1.
# | 场景                            | 期望               |
# | ----------------------------- -| -----------------  |
# | request / notify before start  | InvalidStateError  |
# | request after stop             | InvalidStateError  |
# | start twice                    | InvalidStateError  |
# | stop twice                     | no-op              |
#
# ② 背压
# TEST 2.
# | 场景                     | 期望                             |
# | ----------------------- | ------------------------------   |
# | notify queue full       | BackPressureError                |
# | request queue full      | BackPressureError + pending 清理  |
# | server reply queue full | response 被 drop，不 crash        |
#
# ③ 超时/挂起
# TEST 3.
# | 场景                     | 期望                      |
# | ---------------------   | --------------------     |
# | request timeout         | RequestTimeoutError      |
# | timeout 后 response 到达 | 被忽略，不 set_result      |
# | stop 时有 pending        | pending futures 全部失败  |
#
# ④ JSON-RPC 协议异常
# | 场景                    | 期望           |
# | ---------------------  | ------------   |
# | invalid json           | ignore         |
# | invalid utf-8          | ignore         |
# | unmatched response id  | warning        |
# | response error         | JsonRpcError   |
#
# ⑤ 传输层
# | 场景        | 期望                    |
# | --------   | ---------------------   |
# | send 抛异常 | TransportError → stop   |
# | recv 抛异常 | TransportError → stop   |
#
# ⑥ handler 行为
# | 场景              | 期望                 |
# | ---------------  | -----------------    |
# | handler 抛异常    | 返回 JSON-RPC error  |
# | handler 很慢      | recv_loop 不阻塞     |
# | handler 返回 None | 不发送 response      |


import json
import asyncio
import pytest
import jsonrpc_zmq.exceptions
from jsonrpc_zmq import AsyncJsonRpcPeer
from jsonrpcserver import Success
import pytest_asyncio


@pytest_asyncio.fixture
async def running_server():
    stop_event = asyncio.Event()
    task = asyncio.create_task(server_task(stop_event))

    # 等 server bind 完
    await asyncio.sleep(0.1)

    # 在 pytest fixture 里，yield 只“交权”一次给测试函数。
    # yield 之前：setup
    # yield 那一刻：把控制权交给 test
    # yield 之后：teardown（只执行一次）
    yield

    # teardown
    stop_event.set()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def server_task(stop_event: asyncio.Event):
    server = AsyncJsonRpcPeer(
        "tcp://127.0.0.1:5556",
        bind=True,
    )
    server.start()

    try:
        await stop_event.wait()
    finally:
        await server.stop()


# TEST 1. 测试所有无效状态
@pytest.mark.asyncio
async def test_invalid_state_errors():
    # =========================
    # 1. request / notify before start
    # =========================
    client = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)

    for method in [client.notify, client.request]:
        with pytest.raises(jsonrpc_zmq.exceptions.InvalidStateError,
                           match="peer must be started first"):
            await method("add", {"a": 1, "b": 2})

    await client.stop()  # 安全调用 stop

    # =========================
    # 2. request after stop
    # =========================
    client2 = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)
    client2.start()
    await client2.stop()  # 停止

    for method in [client2.notify, client2.request]:
        with pytest.raises(jsonrpc_zmq.exceptions.InvalidStateError,
                           match="peer already stopped; create a new instance"):
            await method("add", {"a": 1, "b": 2})

    # =========================
    # 3. start twice
    # =========================
    client3 = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)
    client3.start()
    with pytest.raises(jsonrpc_zmq.exceptions.InvalidStateError,
                       match="peer already started"):
        client3.start()
    await client3.stop()

    # =========================
    # 4. stop twice
    # =========================
    client4 = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)
    client4.start()
    await client4.stop()
    assert client4.stopped()
    await client4.stop()


# TEST 2. 背压：server reply queue full → response 被 drop，不 crash
@pytest.mark.asyncio
async def test_server_reply_queue_full_drop_response(monkeypatch):
    server = AsyncJsonRpcPeer(
        "tcp://127.0.0.1:5556",
        bind=True,
        send_queue_maxsize=1,
    )

    async def echo(x):
        return Success(x)

    server.add_handler("echo", echo)
    server.start()

    # 强制 send_queue.put_nowait 抛 QueueFull
    def always_full(_):
        raise asyncio.QueueFull

    monkeypatch.setattr(server._send_queue, "put_nowait", always_full)

    client = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)
    client.start()

    # response 被 drop → client timeout
    with pytest.raises(jsonrpc_zmq.exceptions.RequestTimeoutError):
        await client.request("echo", {"x": 1}, timeout=0.2)

    # server 不 crash
    assert not server.stopped()

    await client.stop()
    await server.stop()



# TEST 3. timeout 后 response 到达 → 被忽略，不 set_result
# 思路
# 1. handler sleep 0.2s
# 2. client timeout=0.05
# 3. timeout 触发 → pending 被 pop
# 4. response 到达 → unmatched response → ignored
@pytest.mark.asyncio
async def test_response_arrives_after_timeout_is_ignored():
    server = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=True)

    async def slow():
        await asyncio.sleep(0.2)
        return Success(42)

    server.add_handler("slow", slow)
    server.start()

    client = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)
    client.start()

    with pytest.raises(jsonrpc_zmq.exceptions.RequestTimeoutError):
        await client.request("slow", timeout=0.05)

    # 等 response 真正返回（但已经没人等了）
    await asyncio.sleep(0.3)

    # pending 必须是干净的
    assert client._pending == {}

    await client.stop()
    await server.stop()


# ====== JSON-RPC 协议异常 =============
@pytest.mark.asyncio
async def test_invalid_json_ignored(running_server):
    client = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)
    client.start()

    # 直接往 socket 发垃圾
    await client._socket.send(b"{not-json")

    # 不 crash
    await asyncio.sleep(0.05)

    await client.stop()


@pytest.mark.asyncio
async def test_invalid_utf8_ignored(running_server):
    client = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)
    client.start()

    await client._socket.send(b"\xff\xfe\xfd")

    await asyncio.sleep(0.05)
    await client.stop()


@pytest.mark.asyncio
async def test_unmatched_response_id_ignored(running_server):
    client = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)
    client.start()

    bogus_response = json.dumps({
        "jsonrpc": "2.0",
        "id": "not-exist",
        "result": 123,
    }).encode()

    await client._socket.send(bogus_response)

    await asyncio.sleep(0.05)
    assert client._pending == {}

    await client.stop()


# ⑤ 传输层异常 → TransportError → stop
# send 抛异常
@pytest.mark.asyncio
async def test_send_transport_error_triggers_stop(monkeypatch):
    client = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)
    client.start()

    async def boom(*args, **kwargs):
        raise RuntimeError("send failed")

    monkeypatch.setattr(client._socket, "send", boom)

    client.notify("test")

    await asyncio.sleep(0.1)

    assert client._stopped

    await client.stop()

# recv 抛异常
@pytest.mark.asyncio
async def test_recv_transport_error_triggers_stop(monkeypatch):
    client = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)
    client.start()

    async def boom():
        raise RuntimeError("recv failed")

    monkeypatch.setattr(client._socket, "recv", boom)

    await asyncio.sleep(0.1)

    assert client._stopped
    await client.stop()


# ⑥ handler 行为
# handler 抛异常 → JSON-RPC error
@pytest.mark.asyncio
async def test_handler_exception_returns_jsonrpc_error():
    server = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=True)

    async def boom():
        raise RuntimeError("boom")

    server.add_handler("boom", boom)
    server.start()

    client = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)
    client.start()

    with pytest.raises(jsonrpc_zmq.exceptions.JsonRpcError) as e:
        await client.request("boom")

    assert e.value.code is not None

    await client.stop()
    await server.stop()


# Error response
@pytest.mark.asyncio
async def test_handler_returns_none_no_response():
    server = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=True)

    server.start()

    client = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)
    client.start()

    with pytest.raises(jsonrpc_zmq.exceptions.JsonRpcError):
        await client.request("noop", timeout=0.1)

    await client.stop()
    await server.stop()
