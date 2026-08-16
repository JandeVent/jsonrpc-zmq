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
