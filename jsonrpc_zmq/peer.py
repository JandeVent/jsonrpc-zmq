# -*- coding: utf-8 -*-
# Copyright (C) 2018-2026 Connet Information Technology Company, Shanghai.

import asyncio
import logging
from functools import partial
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, Union, cast

import zmq
import zmq.asyncio
from jsonrpcclient.requests import id_generators, json, notification, request_impure
from jsonrpcclient.responses import Error, Ok, Response
from jsonrpcclient.responses import parse as parse_response
from jsonrpcserver import async_dispatch, Result

from .exceptions import (BackPressureError, InvalidStateError, JsonRpcError,
                         RequestTimeoutError, TransportError)

# ---------------------------
# Async JSON-RPC Peer
# ---------------------------
logger = logging.getLogger("JsonRpcPeer")


class JsonRpcPeer:
    """
    Asyncio + zmq.asyncio duplex JSON-RPC Peer.

    - zmq_addr: "tcp://*:5556" (bind) or "tcp://host:5556" (connect)
    - bind: True -> socket.bind, False -> socket.connect
    - hwm: ZeroMQ SNDHWM (internal queue high-water mark)
    - send_queue_maxsize: asyncio.Queue maxsize for application->transport buffering
    """

    def __init__(
        self,
        zmq_addr: str,
        bind: bool,
        *,
        sock_type=zmq.PAIR,
        send_queue_maxsize: int = 1000,
    ):
        self._ctx = zmq.asyncio.Context.instance()
        self._socket = self._ctx.socket(sock_type)
        self._request_json = partial(request_impure, id_generators.decimal())
        self._notify_json = notification
        self._parse_response_json = parse_response

        if bind:
            self._socket.bind(zmq_addr)
            logger.info("%s bound zmq socket to %s", self, zmq_addr)
        else:
            self._socket.connect(zmq_addr)
            logger.info("%s connected zmq socket to %s", self, zmq_addr)

        # Mailbox - buffer between app and socket (non-blocking for app)
        self._send_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=send_queue_maxsize)

        # pending responses: id -> Future
        self._pending: Dict[int, asyncio.Future] = {}

        self._recv_task: Optional[asyncio.Task] = None
        self._send_task: Optional[asyncio.Task] = None
        self._started = False
        self._stopped = False
        self._handlers: Dict[str, Callable[..., Awaitable[Result]]] = {}

    def _ensure_startable(self):
        if self._stopped:
            raise InvalidStateError(
                "peer already stopped; create a new instance"
            )

        if self._started:
            raise InvalidStateError(
                "peer already started"
            )

    def _ensure_running(self):
        if self._stopped:
            raise InvalidStateError(
                "peer already stopped; create a new instance"
            )
        if not self._started:
            raise InvalidStateError(
                "peer must be started first"
            )

    def last_endpoint(self) -> str:
        return self._socket.last_endpoint

    def start(self) -> None:
        """Start send/recv tasks"""
        self._ensure_startable()
        self._started = True
        self._recv_task = asyncio.create_task(self._recv_loop())
        self._recv_task.set_name("recv_loop")
        self._send_task = asyncio.create_task(self._send_loop())
        self._send_task.set_name("send_loop")
        self._recv_task.add_done_callback(self._on_transport_task_done)
        self._send_task.add_done_callback(self._on_transport_task_done)

    def started(self) -> bool:
        return self._started

    def stopped(self) -> bool:
        return self._stopped

    def add_handler(self, method: str, handler: Callable[..., Awaitable[Result]]):
        self._handlers[method] = handler

    def remove_handler(self, method: str):
        self._handlers.pop(method, None)

    def _on_transport_task_done(self, task: asyncio.Task) -> None:
        try:
            task.result()  # will re-raise if failed
        except asyncio.CancelledError:
            pass
        except Exception as e:
            # 异步触发 stop
            logger.exception("transport crashed")
            asyncio.create_task(self.stop())

    async def stop(self) -> None:
        """Stop tasks and close socket safely"""
        if self._stopped:
            return

        # let _send_task and _recv_task stopped gracefully
        self._stopped = True

        tasks = []
        if self._send_task:
            tasks.append(self._send_task)
        if self._recv_task:
            tasks.append(self._recv_task)

        # cancel tasks politely if task not stopped
        for t in tasks:
            t.cancel()

        # await their completion (ignore CancelledError)
        for t in tasks:
            try:
                await t
            except asyncio.CancelledError:
                logger.info("%s task '%s' cancelled", self, t.get_name())
            except Exception as e:
                logger.error("%s task '%s' crashed: %s", self, t.get_name(), e)
        try:
            self._socket.close(linger=0)
            # do NOT terminate global ctx here; other peers may use it
        except Exception as e:
            logger.exception("%s socket close error: %s", self, e)

        # Optional: fail all pending requests
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(TransportError("transport failed"))
        self._pending.clear()

        logger.info("%s stopped", self)

    # ---------------------------
    # Public API
    # ---------------------------
    async def request(
        self,
        method: str,
        params: Union[Dict[str, Any], Tuple[Any, ...], None] = None,
        timeout: Optional[float] = None
    ) -> Ok:
        """
        Send a JSON-RPC request and await its response.
        - method: method name
        - params: params object or None
        - timeout: seconds to wait for response (None = wait forever)

        :raise RequestTimeoutError: If timeout is exceeded
        :raise BackPressureError: If send queue is full
        :raise TransportError: If transport fails
        :raise JsonRpcError: If remote fails
        """
        self._ensure_running()

        req = self._request_json(method, params)
        msg_id = req["id"]
        data = json.dumps(req).encode("utf-8")

        fut = asyncio.get_running_loop().create_future()
        self._pending[msg_id] = fut

        # try put into send_queue without blocking the app
        try:
            self._send_queue.put_nowait(data)
        except asyncio.QueueFull:
            # clean up pending
            self._pending.pop(msg_id, None)
            raise BackPressureError("send queue full; cannot enqueue request")

        try:
            result = await asyncio.wait_for(fut, timeout) if timeout else await fut
            return result
        except asyncio.TimeoutError as e:
            fut.cancel()
            raise RequestTimeoutError(f"request time out: {e}")
        finally:
            # ensure pending cleaned up if future still present
            self._pending.pop(msg_id, None)

    def notify(
        self,
        method: str,
        params: Union[Dict[str, Any], Tuple[Any, ...], None] = None,
    ) -> None:
        """Send a notification (no response expected)

        Notice: It will be failed and meant to be failed (correct failure) when backpressure is huge.
                The sending path cannot be reversed and cause business logic to fail.
        """
        self._ensure_running()

        req = self._notify_json(method, params)
        data = json.dumps(req).encode("utf-8")
        try:
            self._send_queue.put_nowait(data)
        except asyncio.QueueFull:
            raise BackPressureError("send queue full; cannot enqueue notification")

    # ---------------------------
    # transport loops
    # ---------------------------
    async def _send_loop(self):
        """
        Send loop for Async JSON-RPC over ZMQ.

        Logic summary:
        1. Continuously pull messages from an asyncio queue (`send_queue`) with a short timeout (0.1s),
            which ensures the loop checks `_stopped` at least every 100ms.
        2. Attempt a non-blocking ZMQ send to quickly detect if the internal ZMQ queue is full.
           - If successful, continue to next message.
           - If queue is full (`zmq.Again`), enter slow path.
        3. Slow path: apply a small backoff, then attempt a blocking send with a timeout.
           - If the peer is too slow or disconnected, raise TransportError.
           - Any unexpected exception is propagated as TransportError.
        4. Loop continues until the `_stopped` event is set.
        5. The `finally` block ensures cleanup/logging happens when the loop exits,
           including on cancellation (asyncio.CancelledError) or errors.
        """
        try:
            while True:
                # -----------------------------
                # 1. Fetch message from queue (with timeout to avoid blocking forever)
                #    This also allows checking _stopped at least every 0.1s.
                # -----------------------------
                try:
                    msg = await asyncio.wait_for(self._send_queue.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue  # Queue is empty, continue next iteration

                # -----------------------------
                # 2. Try non-blocking send to quickly detect if ZMQ internal queue is full
                # -----------------------------
                try:
                    await self._socket.send(msg, flags=zmq.NOBLOCK)
                    logger.info("%s send message: %r", self, msg[:100])
                    continue  # Send succeeded, continue loop
                except zmq.Again:
                    # Internal queue full → slow path
                    logger.warning("%s ZMQ internal queue full; retry after backoff", self)
                except Exception as e:
                    raise TransportError(f"{self} unexpected send error: {e}")

                # -----------------------------
                # 3. Slow path: retry with small backoff and timeout
                # -----------------------------
                await asyncio.sleep(0.05)  # Simple backoff
                try:
                    await asyncio.wait_for(self._socket.send(msg), timeout=1.0)
                    logger.info("%s send message: %s", self, msg)
                except asyncio.TimeoutError:
                    # Peer may be dead or extremely slow
                    raise TransportError(f"{self} thinks peer too slow (probably dead)")
                except Exception as e:
                    raise TransportError(f"{self} send failed after retry: {e}")
        finally:
            logger.info("%s send loop task exiting", self)

    async def _recv_loop(self):
        """Receive messages and dispatch: if request -> async_dispatch -> send response;
           if response -> set pending future result/exception.
        """
        try:
            while True:
                try:
                    msg = await self._socket.recv()
                except Exception as e:
                    raise TransportError(f"{self} unexpected receive error: {e}")

                # ===== decode
                try:
                    text = msg.decode("utf-8")
                except Exception:
                    logger.exception("%s invalid bytes received %r", self, msg[:100])
                    continue

                # ====== parse JSON
                try:
                    data = json.loads(text)
                except Exception:
                    logger.exception("%s invalid json received: %r", self, text[:100])
                    continue

                if not isinstance(data, dict):
                    logger.exception("%s invalid json rpc received: %r", self, data)
                    continue  # ignore non-dict messages

                # --- request / notification ---
                if "method" in data:
                    logger.info("%s received request / notification: %r", self, text)
                    asyncio.create_task(
                        self._handle_request(text),
                        name=f"rpc:{data.get('method')}"
                    )

                # --- response ---
                elif "id" in data and ("result" in data or "error" in data):
                    try:
                        logger.info("%s received response: %r", self, text)
                        response: Response = self._parse_response_json(data)
                        fut = self._pending.pop(response.id, None)
                        if fut is None:
                            logger.warning("%s unmatched response: %r", self, response)
                        else:
                            if isinstance(response, Ok):
                                fut.set_result(response.result)
                            elif isinstance(response, Error):
                                fut.set_exception(JsonRpcError(response.code, response.message, response.data))
                    except Exception as e:
                        logger.exception("%s invalid json rpc response: %r", self, data)
            # !while

        finally:
            logger.info("%s receive loop task exiting", self)

    async def _handle_request(self, text: str):
        try:
            response_s = await async_dispatch(text, methods=self._handlers)
            if response_s:
                self._send_queue.put_nowait(response_s.encode("utf-8"))
        except asyncio.QueueFull:
            logger.error("%s send queue full; drop response: %r" % (self, response_s))
        except Exception as e:
            logger.exception("%s request handle error: %s", self, e)
