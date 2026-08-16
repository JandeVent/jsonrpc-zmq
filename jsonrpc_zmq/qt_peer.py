# -*- coding: utf-8 -*-
# Copyright (C) 2018-2026 Connet Information Technology Company, Shanghai.


import logging
import threading
import time
from collections import deque
from functools import partial
from typing import Any, Callable, Deque, Dict, Optional, Tuple, Union

import zmq
from jsonrpcclient.requests import (id_generators, json, notification,
                                    request_impure)
from jsonrpcclient.responses import Error, Ok, Response, parse_json
from jsonrpcserver import Result, dispatch

from qtpy.QtCore import (QEventLoop, QMetaObject, QObject, QSocketNotifier,
                         QThread, QTimer, Qt, Signal, Slot)
from .exceptions import JsonRpcError, RequestTimeoutError, TransportError

logger = logging.getLogger("QJsonRpcPeer")


# -------------------------
# PendingRequest
# -------------------------
class QPendingRequest(QObject):
    def __init__(self):
        super().__init__()
        self._result = None
        self._exception = None
        self._loop = QEventLoop()
        self._event = threading.Event()
        self._done = False

    def done(self) -> bool:
        return self._done

    def wait(self, timeout: Optional[float]=None):
        deadline = time.monotonic() + timeout if timeout else None
        if threading.current_thread() is threading.main_thread():
            # main thread: pump events so the server-side requestReceived
            # dispatch and the response notification can be processed.
            # Bounded pumping (maxTime) guarantees the wait terminates even
            # if a cross-thread wakeup is lost; the response itself is
            # resolved in the IO thread and _done is set directly.
            while not self._done:
                if deadline is not None and time.monotonic() >= deadline:
                    break
                self._loop.processEvents(QEventLoop.ProcessEventsFlag.WaitForMoreEvents, 50)
        else:
            # worker thread: the response is resolved by the IO thread; a
            # threading.Event wakeup is 100% reliable (unlike a cross-thread
            # QEventLoop.quit(), which can occasionally fail to wake a
            # blocked exec_()).
            self._event.wait(timeout)
        self._done = True

    def set_result(self, result: Any):
        self._result = result
        self._done = True
        self._event.set()

    def set_exception(self, exc: Exception):
        self._exception = exc
        self._done = True
        self._event.set()

    def exception(self) -> Optional[Exception]:
        return self._exception

    def result(self) -> Optional[Any]:
        return self._result


# -------------------------
# SendWorker
# -------------------------
class SendWorker(QObject):

    exceptionOccurred = Signal(Exception) # any exception happened in worker
    _kick = Signal()  # internal: wake the send loop in the IO thread (queued)

    def __init__(self, zmq_socket: zmq.Socket):
        super().__init__()
        self._socket: zmq.Socket = zmq_socket
        self._send_queue: Deque[bytes] = deque(maxlen=1000)
        self._retry_timer: Optional[QTimer] = None  # created in IO thread (start)
        self._kick.connect(self._send_loop)

    @Slot()
    def start(self):
        """Called in the IO thread (queued) after the thread starts."""
        # Retry timer: only active while the peer is slow (zmq.Again backpressure)
        self._retry_timer = QTimer(self)
        self._retry_timer.setInterval(10)
        self._retry_timer.timeout.connect(self._send_loop)

    @Slot()
    def shutdown(self):
        """Called in the IO thread before the thread quits."""
        if self._retry_timer is not None:
            self._retry_timer.stop()

    def send_message(self, msg: bytes):
        """主线程调用"""
        if len(self._send_queue) >= self._send_queue.maxlen:
            self.exceptionOccurred.emit(TransportError("peer too slow (probably dead)"))
            return

        self._send_queue.append(msg)
        self._kick.emit()  # wake the IO thread immediately (queued connection)

    @Slot()
    def _send_loop(self):
        while self._send_queue:
            msg = self._send_queue[0]
            try:
                self._socket.send(msg, flags=zmq.NOBLOCK)
                logger.debug(f"send message: {msg}")
                self._send_queue.popleft()
            except zmq.Again:
                # peer slow: retry on a short timer instead of busy-polling
                if self._retry_timer is not None:
                    self._retry_timer.start()
                return
            except Exception as e:
                self.exceptionOccurred.emit(e)
                return
        # queue drained
        if self._retry_timer is not None:
            self._retry_timer.stop()


# -------------------------
# RecvWorker
# -------------------------
class RecvWorker(QObject):

    responseReceived = Signal(str)  # response
    requestReceived = Signal(str)  # request / notification
    exceptionOccurred = Signal(Exception) # any exception happened in worker

    def __init__(self, zmq_socket, on_response=None):
        super().__init__()
        self._parse_response_json = parse_json
        self._socket: zmq.Socket = zmq_socket
        self._on_response: Optional[Callable[[str], None]] = on_response
        self._notifier: Optional[QSocketNotifier] = None  # created in IO thread (start)
        self._safety_timer: Optional[QTimer] = None  # created in IO thread (start)

    @Slot()
    def start(self):
        """Called in the IO thread (queued) after the thread starts."""
        fd = self._socket.getsockopt(zmq.FD)
        self._notifier = QSocketNotifier(fd, QSocketNotifier.Type.Read, self)
        self._notifier.activated.connect(self._recv_loop)
        # Safety net: re-poll occasionally in case the notifier misses an event
        # (a rare OS-level wakeup race; a missed activation is otherwise
        # bounded by this timer). 250ms keeps idle CPU negligible while
        # bounding a rare latency spike to ~250ms.
        self._safety_timer = QTimer(self)
        self._safety_timer.setInterval(250)
        self._safety_timer.timeout.connect(self._recv_loop)
        self._safety_timer.start()

    @Slot()
    def shutdown(self):
        """Called in the IO thread before the thread quits."""
        if self._safety_timer is not None:
            self._safety_timer.stop()
        if self._notifier is not None:
            self._notifier.setEnabled(False)

    @Slot()
    def _recv_loop(self):
        try:
            while True:
                try:
                    msg = self._socket.recv(flags=zmq.NOBLOCK)
                except zmq.Again:
                    break  # 暂时没有消息
                except Exception as e:
                    self.exceptionOccurred.emit(TransportError(f"unexpected receive error: {e}"))
                    break

                try:
                    text = msg.decode("utf-8")
                except Exception:
                    logger.exception("invalid bytes received")
                    continue

                try:
                    data = json.loads(text)
                except Exception:
                    logger.exception("invalid json received: %r", text)
                    continue

                if not isinstance(data, dict):
                    logger.exception("invalid json rpc received: %r", data)
                    continue  # ignore non-dict messages

                # --- request / notification ---
                if "method" in data:
                    logger.debug(f"received request / notification: {text}")
                    self.requestReceived.emit(text)

                # --- response ---
                elif "id" in data and ("result" in data or "error" in data):
                    logger.debug(f"received response: {text}")
                    # resolve the pending request IN THE IO THREAD so the
                    # request path never depends on the main thread's event
                    # loop (whose event delivery can occasionally be delayed)
                    if self._on_response is not None:
                        try:
                            self._on_response(text)
                        except Exception:
                            logger.exception("response resolution error")
                    self.responseReceived.emit(text)

        except Exception as e:
            self.exceptionOccurred.emit(e)
        finally:
            # re-arm the notifier (Qt disables it after activation)
            if self._notifier is not None:
                self._notifier.setEnabled(True)


# -------------------------
# JsonRpcPeer in Qt
# -------------------------
class QJsonRpcPeer(QObject):

    def __init__(self, zmq_addr: str, bind=True):
        super().__init__()

        self._ctx = zmq.Context()
        self._socket = self._ctx.socket(zmq.PAIR)

        self._request_json = partial(request_impure, id_generators.decimal())
        self._notify_json = notification
        self._parse_response_json = parse_json


        if bind:
            self._socket.bind(zmq_addr)
            logger.debug("bound zmq socket to %s", zmq_addr)
        else:
            self._socket.connect(zmq_addr)
            logger.debug("connected zmq socket to %s", zmq_addr)

        # pending request dict
        self._pending: Dict[int, QPendingRequest] = {}

        self._started = False
        self._stopped = False
        self._handlers: Dict[str, Callable[..., Result]] = {}

        # -------------------------
        # IO thread
        # -------------------------
        self._io_thread = QThread()

        self._send_worker = SendWorker(self._socket)
        self._send_worker.moveToThread(self._io_thread)
        self._send_worker.exceptionOccurred.connect(self.exceptionOccurred)

        self._recv_worker = RecvWorker(self._socket, on_response=self._resolve_response)
        self._recv_worker.moveToThread(self._io_thread)
        self._recv_worker.responseReceived.connect(self.responseReceived)
        self._recv_worker.requestReceived.connect(self.requestReceived)
        self._recv_worker.exceptionOccurred.connect(self.exceptionOccurred)

    def last_endpoint(self) -> str:
        return self._socket.last_endpoint

    def started(self) -> bool:
        return self._started

    def stopped(self) -> bool:
        return self._stopped

    def start(self):
        self._started = True
        self._io_thread.start()
        # create notifiers/timers inside the IO thread
        QMetaObject.invokeMethod(self._send_worker, "start", Qt.ConnectionType.QueuedConnection)
        QMetaObject.invokeMethod(self._recv_worker, "start", Qt.ConnectionType.QueuedConnection)

    def stop(self):
        self._stopped = True
        logger.debug("io thread quiting")
        if self._io_thread.isRunning():
            # stop timers/notifiers inside the IO thread to avoid
            # cross-thread timer destruction warnings
            QMetaObject.invokeMethod(self._send_worker, "shutdown", Qt.ConnectionType.BlockingQueuedConnection)
            QMetaObject.invokeMethod(self._recv_worker, "shutdown", Qt.ConnectionType.BlockingQueuedConnection)
        self._io_thread.quit()
        self._io_thread.wait()
        try:
            self._socket.close(linger=0)
            # do NOT terminate global ctx here; other peers may use it
        except Exception:
            logger.exception("socket close error")

        # Optional: fail all pending requests
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(TransportError("transport failed"))
        self._pending.clear()

        logger.debug("stopped")

    # -------------------------
    # Public API
    # -------------------------
    def add_handler(self, method: str, handler: Callable[..., Result]):
        self._handlers[method] = handler

    def remove_handler(self, method: str):
        self._handlers.pop(method, None)

    def request(
        self,
        method: str,
        params: Union[Dict[str, Any], Tuple[Any, ...], None] = None,
        timeout: Optional[float] = None
    ) -> Any:
        """
        Send a JSON-RPC request and await its response.
        - method: method name
        - params: params object or None
        - timeout: seconds to wait for response (None = wait forever)

        :raise RequestTimeoutError: If timeout is exceeded
        :raise TransportError: If transport fails
        :raises: JsonRpcError
        """
        req = self._request_json(method, params)
        msg_id = req["id"]
        data = json.dumps(req).encode("utf-8")

        pending = QPendingRequest()
        self._pending[msg_id] = pending

        # 发给发送线程
        self._send_worker.send_message(data)
        # 阻塞等待响应
        try:
            pending.wait(timeout)
            if pending.exception() is None and pending.result() is None:
                raise RequestTimeoutError(f"request time out: {req}")
            elif pending.exception() is not None:
                raise pending.exception()
            elif pending.result() is not None:
                return pending.result()
        finally:
            self._pending.pop(msg_id, None)

    def notify(
        self,
        method: str,
        params: Union[Dict[str, Any], Tuple[Any, ...], None] = None,
    ) -> None:
        """Send a notification (no response expected)
        """
        req = self._notify_json(method, params)
        data = json.dumps(req).encode("utf-8")
        self._send_worker.send_message(data)

    # -------------------------
    # 内部 slot
    # -------------------------
    def _resolve_response(self, text: str):
        """Resolve a pending request. Runs in the IO thread (recv worker)."""
        response: Response = self._parse_response_json(text)
        fut: QPendingRequest = self._pending.get(response.id, None)
        if fut is None:
            logger.warning("unmatched response: %r", response)
        else:
            if isinstance(response, Ok):
                fut.set_result(response.result)
            elif isinstance(response, Error):
                fut.set_exception(JsonRpcError(response.code, response.message, response.data))

    @Slot(str)
    def responseReceived(self, text: str):
        # application-level notification only; the request is already
        # resolved in the IO thread
        logger.debug("response delivered: %r", text)

    @Slot(str)
    def requestReceived(self, text: str):
        try:
            response_s = dispatch(text, methods=self._handlers)
            if response_s:
                self._send_worker.send_message(response_s.encode("utf-8"))
        except Exception as e:
            logger.exception(f"request handle error: {e}")

    @Slot(Exception)
    def exceptionOccurred(self, e: Exception):
        logger.exception("exception occurred: %r", e)
        self.stop()