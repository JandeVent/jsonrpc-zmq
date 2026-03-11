import asyncio
from jsonrpc_zmq import AsyncJsonRpcPeer, Success, Error


async def pong():
    return Success("Pong")


async def server_main():
    server = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=True)
    server.add_handler("ping", pong)
    server.start()
    await asyncio.Event().wait()
    await server.stop()

asyncio.run(server_main())