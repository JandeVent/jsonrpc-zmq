# -*- coding: utf-8 -*-
# Copyright (C) 2018-2026 Connet Information Technology Company, Shanghai.


import logging
from collections import deque
from functools import partial
from typing import Any, Callable, Deque, Dict, Optional, Tuple, Union

import zmq
from jsonrpcclient.requests import (id_generators, json, notification,
                                    request_impure)
from jsonrpcclient.responses import Error, Ok, Response, parse_json
from jsonrpcserver import Result, dispatch

from qtpy.QtCore import QEventLoop, QObject, QThread, QTimer, Signal, Slot, QSocketNotifier
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
        self._done = False

    def done(self) -> bool:
        return self._done

    def wait(self, timeout: Optional[float]=None):
        if timeout:
            timer = QTimer()
            timer.setSingleShot(True)
            timer.timeout.connect(self._loop.quit)
            timer.start(int(timeout * 1000))
        self._loop.exec_()
        self._done = True

    def set_result(self, result: Any):
        self._result = result
        self._loop.quit()

    def set_exception(self, exc: Exception):
        self._exception = exc
        self._loop.quit()

    def exception(self) -> Optional[Exception]:
        return self._exception

    def result(self) -> Optional[Any]:
        return self._result


# -------------------------
# SendWorker
# -------------------------
class SendWorker(QObject):

    exceptionOccurred = Signal(Exception) # any exception happened in worker

    def __init__(self, zmq_socket: zmq.Socket):
        super().__init__()
        self._socket: zmq.Socket = zmq_socket
        self._send_queue: Deque[bytes] = deque(maxlen=1000)
        self._timer = QTimer()  # 放在主线程
        self._timer.timeout.connect(self._send_loop)
        self._timer.start(10)  # check send queue and kick send loop every 10ms

    def send_message(self, msg: bytes):
        """主线程调用"""
        if len(self._send_queue) >= self._send_queue.maxlen:
            self.exceptionOccurred.emit(TransportError("peer too slow (probably dead)"))
            return

        self._send_queue.append(msg)

    @Slot()
    def _send_loop(self):
        while self._send_queue:
            msg = self._send_queue[0]
            try:
                self._socket.send(msg, flags=zmq.NOBLOCK)
                logger.debug(f"send message: {msg}")
                self._send_queue.popleft()
            except zmq.Again:
                break
            except Exception as e:
                self.exceptionOccurred.emit(e)
                break


# -------------------------
# RecvWorker
# -------------------------
class RecvWorker(QObject):

    responseReceived = Signal(str)  # response
    requestReceived = Signal(str)  # request / notification
    exceptionOccurred = Signal(Exception) # any exception happened in worker

    def __init__(self, zmq_socket):
        super().__init__()
        self._parse_response_json = parse_json
        self._socket: zmq.Socket = zmq_socket
        self._notifier: Optional[QSocketNotifier] = None
        self._timer = QTimer() # 放在主线程
        self._timer.timeout.connect(self._recv_loop)
        self._timer.start(10) # check send queue and kick receive loop every 10ms

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
                    self.responseReceived.emit(text)

        except Exception as e:
            self.exceptionOccurred.emit(e)


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

        self._recv_worker = RecvWorker(self._socket)
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

    def stop(self):
        self._stopped = True
        logger.debug("io thread quiting")
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
    @Slot(str)
    def responseReceived(self, text: str):
        try:
            response: Response = self._parse_response_json(text)
            fut: QPendingRequest = self._pending.get(response.id, None)
            if fut is None:
                logger.warning("unmatched response: %r", response)
            else:
                if isinstance(response, Ok):
                    fut.set_result(response.result)
                elif isinstance(response, Error):
                    fut.set_exception(JsonRpcError(response.code, response.message, response.data))

        except Exception as e:
            logger.exception("invalid json rpc received: %r", e)

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