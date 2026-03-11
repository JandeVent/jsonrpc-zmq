
import sys
from qtpy.QtWidgets import QApplication
from qtpy.QtCore import QTimer
from jsonrpc_zmq import QJsonRpcPeer

app = None
peer: QJsonRpcPeer = None

def main():
    global peer
    peer = QJsonRpcPeer("tcp://127.0.0.1:5556", bind=False)
    peer.start()


def request_and_quit():
    global peer, app
    result = peer.request("ping")
    print(result)
    peer.stop()
    app.quit()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    main()
    QTimer.singleShot(1000, lambda: request_and_quit())
    app.exec_()