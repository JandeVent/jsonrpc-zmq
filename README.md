# jsonrpc-zmq

**jsonrpc-zmq** is a lightweight **duplex JSON-RPC 2.0 peer implementation** built on top of **ZeroMQ**.

Unlike traditional **client/server RPC frameworks**, this project implements a **peer-to-peer JSON-RPC transport**, meaning **both sides can send requests and notifications**.

The library provides two runtimes:

* **Async Runtime** – built on `asyncio`
* **Qt Runtime** – built on `Qt event loop`

This allows JSON-RPC communication in:

* asyncio applications
* Qt GUI applications
* distributed systems
* simulation and testing frameworks
* IPC tools

---

# Features

* JSON-RPC 2.0 compliant
* peer-to-peer RPC
* ZeroMQ transport
* request / response
* notification support
* handler registration
* request timeout support
* backpressure protection
* bounded send queue
* graceful shutdown
* detailed logging

---


Two runtime implementations share the same protocol semantics.

---

# Architecture

```text
Application
      │
      │ request / notify
      ▼
JsonRpcPeer / QJsonRpcPeer
      │
      │
      ▼
ZeroMQ Socket
      │
      ▼
Remote Peer
```

Each peer can:

* send requests
* receive requests
* send notifications
* receive notifications

---

# Transport Model

Both implementations share the same conceptual transport model.

```text
Application
   │
   │ request()
   │ notify()
   ▼
Send Queue
   │
   ▼
Transport Loop
   │
   ▼
ZeroMQ Socket
   │
   ▼
Receive Loop
   │
   ▼
dispatch / resolve pending
```

---

# Installation

Install the package (dependencies are pulled in automatically):

```bash
pip install .
```

Dependencies:

| library       | purpose            |
| ------------- | ------------------ |
| pyzmq         | transport          |
| jsonrpcclient | request generation |
| jsonrpcserver | request dispatch   |
| qtpy          | Qt abstraction     |

---

# Runtime Implementations

## Async Runtime

Module:

```text
JsonRpcPeer
```

Built on:

```text
asyncio
zmq.asyncio
```

### Async architecture

```text
Application coroutine
        │
        │ await request()
        ▼
send_queue (asyncio.Queue)
        │
        ▼
send_loop task
        │
        ▼
ZeroMQ socket
        │
        ▼
recv_loop task
        │
        ▼
resolve Future / dispatch handler
```

### Example

Server:

```python
import asyncio
import logging

from jsonrpc_zmq import AsyncJsonRpcPeer, Success

logging.basicConfig(level=logging.INFO)


async def pong(ss=None):
    """ping handler: echoes back the optional 'ss' parameter"""
    return Success("pong" if ss is None else f"pong: {ss}")


async def echo(msg=None):
    print("received notification:", msg)
    return Success(msg)


async def server_main():
    server = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=True)
    server.add_handler("ping", pong)
    server.add_handler("echo", echo)
    server.start()
    try:
        await asyncio.Event().wait()
    finally:
        await server.stop()


try:
    asyncio.run(server_main())
except KeyboardInterrupt:
    print("server stopped")
```

Client:

```python
import asyncio
import logging

from jsonrpc_zmq import AsyncJsonRpcPeer, JsonRpcError, RequestTimeoutError

logging.basicConfig(level=logging.DEBUG)


async def client_main():
    client = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)
    client.start()
    try:
        # request with params; the server's ping handler echoes the value back
        try:
            res = await client.request("ping", {"ss": 1}, timeout=3)
            print("got:", res)
        except JsonRpcError as e:
            print("Remote error:", e)
        except RequestTimeoutError as e:
            print("Request timeout:", e)

        # plain request
        try:
            res = await client.request("ping", timeout=3)
            print("got:", res)
        except JsonRpcError as e:
            print("Remote error:", e)
        except RequestTimeoutError as e:
            print("Request timeout:", e)

        # notification (fire-and-forget; the server prints what it receives)
        client.notify("echo", {"msg": "hello"})
        print("sleeping 10 seconds...")
        await asyncio.sleep(10)
    finally:
        await client.stop()


try:
    asyncio.run(client_main())
except KeyboardInterrupt:
    pass
```

Output:

```
got: pong: 1
got: pong
sleeping 10 seconds...
```

---

## Qt Runtime

Module:

```text
qt.QJsonRpcPeer
```

Built on:

```text
Qt event loop
QThread
QSocketNotifier
```

### Qt architecture

```text
Main Thread
   │
   │ request()
   │ notify()
   ▼
QJsonRpcPeer
   │
   └── IO Thread
        │
        ├── SendWorker
        │
        └── RecvWorker
```

IO is handled in a dedicated **Qt thread**.

### Example

Server:

```python

import sys
from qtpy.QtWidgets import QApplication
from jsonrpc_zmq import QJsonRpcPeer, Success

app = None
peer = None

def ping(ss=None):
    return Success("pong" if ss is None else f"pong: {ss}")

def main():
    global peer
    peer = QJsonRpcPeer("tcp://127.0.0.1:5556", bind=True)
    peer.add_handler("ping", ping)
    peer.start()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    main()
    try:
        rc = app.exec_()
    except KeyboardInterrupt:
        rc = 0
    peer.stop()
    sys.exit(rc)
```

Client:

```python

import sys
from qtpy.QtCore import QTimer
from qtpy.QtWidgets import QApplication
from jsonrpc_zmq import JsonRpcError, QJsonRpcPeer, RequestTimeoutError

app = None
peer = None

def main():
    global peer
    peer = QJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)
    peer.start()


def request_and_quit():
    global peer, app
    try:
        result = peer.request("ping", timeout=3)
        print("got:", result)
    except JsonRpcError as e:
        print("Remote error:", e)
    except RequestTimeoutError as e:
        print("Request timeout:", e)
    finally:
        peer.stop()
        app.quit()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    main()
    QTimer.singleShot(1000, request_and_quit)
    try:
        app.exec_()
    except KeyboardInterrupt:
        pass
```

---

# Requests

Send request:

```python
peer.request("method", params)
```

Async version:

```python
await peer.request("method", params)
```

---

# Notifications

Send notification:

```python
peer.notify("event", {"data": 1})
```

Notifications do not expect responses.

---

# Handler Registration

Register RPC method:

```python
peer.add_handler("multiply", handler)
```

Remove handler:

```python
peer.remove_handler("multiply")
```

---

# Timeout

Requests support timeout.

Async:

```python
await peer.request("slow", timeout=2)
```

Qt:

```python
peer.request("slow", timeout=2)
```

If exceeded:

```
RequestTimeoutError
```

is raised.

---

# Backpressure

Outgoing messages are buffered in a **bounded queue**.

Async version:

```text
asyncio.Queue(maxsize=N)
```

Qt version:

```text
deque(maxlen=N)
```

If the queue is full:

```
BackPressureError
TransportError
```

may be raised.

---

# Error Model

Possible exceptions:

| Exception           | Meaning                     |
| ------------------- | --------------------------- |
| InvalidStateError   | peer not started or stopped |
| RequestTimeoutError | request timed out           |
| BackPressureError   | send queue full             |
| TransportError      | transport failure           |
| JsonRpcError        | remote RPC error            |

Example:

```python
try:
    result = await peer.request("foo")
except JsonRpcError as e:
    print(e)
```

---

# Shutdown

Async version:

```python
await peer.stop()
```

Qt version:

```python
peer.stop()
```

Shutdown will:

* stop transport loops
* close socket
* fail all pending requests

---

# Logging

The library uses **Python logging**.

Enable debug logs:

```python
import logging

logging.basicConfig(level=logging.DEBUG)
```

Example output:

```
INFO JsonRpcPeer send message
INFO JsonRpcPeer received response
ERROR JsonRpcPeer transport crashed
```

---

# Design Goals

The project focuses on:

* simplicity
* predictable failure model
* runtime portability (async / Qt)
* safe backpressure handling
* minimal dependencies

This project is intentionally **not a full RPC framework**.

---

# Limitations

Current limitations:

* single ZMQ socket
* no reconnect logic
* no batching
* no authentication
* recommended socket type: `PAIR`

---

# Example Use Cases

* distributed testing frameworks
* simulation systems
* Qt desktop tools
* CLI automation tools
* IPC between processes


## License

Copyright (C) 2018-2026
Connet Information Technology Company, Shanghai.

