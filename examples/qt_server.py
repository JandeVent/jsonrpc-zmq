
import sys
from qtpy.QtWidgets import QApplication
from jsonrpc_zmq import QJsonRpcPeer, Success

app = None
peer = None

def ping():
    return Success("pong")

def main():
    global peer
    peer = QJsonRpcPeer("tcp://127.0.0.1:5556", bind=True)
    peer.add_handler("ping", ping)
    peer.start()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    main()
    app.exec_()