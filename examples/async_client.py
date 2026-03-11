import asyncio
from jsonrpc_zmq import AsyncJsonRpcPeer, JsonRpcError, RequestTimeoutError

import logging
logging.basicConfig(level=logging.DEBUG)

async def client_main():
    client = AsyncJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)
    client.start()
    # call server's ping
    try:
        res = await client.request("ping", {'ss': 1})
    except JsonRpcError:
        print("Remote error")
    except RequestTimeoutError:
        print("Request timeout")

    res = await client.request("ping")
    print("got:", res)
    # send notification
    client.notify("echo", {"msg": "hello"})
    print("sleeping 10 seconds...")
    await asyncio.sleep(10)
    await client.stop()

asyncio.run(client_main())

