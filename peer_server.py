# peer_server.py
import socket
import threading
from typing import Callable, Optional, List

from peer_logger import PeerLogger
from piece_manager import PieceManager
from peer_handler import PeerHandler

class PeerServer:
    """
    TCP listener that accepts inbound connections and spawns a PeerHandler per neighbor [11].
    Provides a broadcast_have callback to handlers so a received piece can be announced to others [8].
    """
    def __init__(
        self,
        *,
        host: str,
        port: int,
        my_peer_id: int,
        logger: PeerLogger,
        piece_mgr: PieceManager,
        allows_upload_to: Optional[Callable[[int], bool]] = None,
    ):
        self.host = host
        self.port = port
        self.my_peer_id = my_peer_id
        self.logger = logger
        self.piece_mgr = piece_mgr
        self.allows_upload_to = allows_upload_to or (lambda _peer_id: True)

        self._server_sock: Optional[socket.socket] = None
        self._handlers: List[PeerHandler] = []
        self._lock = threading.Lock()
        self._dead = False

    def start(self) -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen()
        self._server_sock = srv

        threading.Thread(target=self._accept_loop, daemon=True).start()

    def stop(self) -> None:
        self._dead = True
        try:
            if self._server_sock:
                self._server_sock.close()
        except OSError:
            pass
        # Handlers will exit when their sockets close.

    # ---- internal ----

    def _broadcast_have(self, origin: PeerHandler, piece_index: int) -> None:
        with self._lock:
            for h in list(self._handlers):
                if h is not origin:
                    h.send_have(piece_index)

    def _accept_loop(self) -> None:
        assert self._server_sock is not None
        while not self._dead:
            try:
                client_sock, _addr = self._server_sock.accept()
            except OSError:
                break
            handler = PeerHandler(
                sock=client_sock,
                my_peer_id=self.my_peer_id,
                logger=self.logger,
                piece_mgr=self.piece_mgr,
                outgoing=False,                 # inbound
                expected_peer_id=None,          # unknown until handshake [8]
                broadcast_have=lambda idx, origin=None: self._broadcast_have(origin or handler, idx),
                allows_upload_to=self.allows_upload_to,
            )
            with self._lock:
                self._handlers.append(handler)

            def run_and_cleanup(h: PeerHandler):
                try:
                    h.run()
                finally:
                    with self._lock:
                        if h in self._handlers:
                            self._handlers.remove(h)

            threading.Thread(target=run_and_cleanup, args=(handler,), daemon=True).start()