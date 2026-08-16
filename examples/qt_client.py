
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
