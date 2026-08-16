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
