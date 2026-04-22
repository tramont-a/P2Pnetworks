# peer_admin.py
import os
import socket
import threading
from typing import Dict, Optional, Set, List

from common_config import CommonConfig
from peer_info_config import PeerInfoConfig, RemotePeerInfo
from peer_logger1 import PeerLogger
from piece_manager import PieceManager, Bitfield
from peer_server import PeerServer
from peer_handler import PeerHandler
from schedulers import ChokeScheduler, OptimisticUnchokeScheduler, TerminateWatcher


class PeerAdmin:
    """
    Orchestrates a peer: config, storage, server, neighbor handlers, and schedulers.
    Translated from PeerAdmin.java [19].
    """

    def __init__(self, peer_id: int):
        self.my_id = int(peer_id)

        # Config
        self.common = CommonConfig()
        self.peers_cfg = PeerInfoConfig()

        # My info and peer list
        self.my_row: Optional[RemotePeerInfo] = None
        self.peer_map: Dict[int, RemotePeerInfo] = {}
        self.peer_list: List[int] = []

        # Storage
        self.peer_dir: Optional[str] = None
        self.piece_mgr: Optional[PieceManager] = None

        # Networking
        self._server: Optional[PeerServer] = None
        self._handlers: Dict[int, PeerHandler] = {}
        self._lock = threading.Lock()

        # Scheduler state
        self._unchoked: Set[int] = set()
        self._optimistic: Optional[int] = None

        # Tracking neighbor availability (optional; filled by callbacks if wired)
        self._neighbor_bf: Dict[int, Bitfield] = {}

        # Logger
        self.logger = PeerLogger(str(self.my_id))

        # Control
        self._stop_ev = threading.Event()
        self._choke_sched: Optional[ChokeScheduler] = None
        self._opt_sched: Optional[OptimisticUnchokeScheduler] = None
        self._term_watcher: Optional[TerminateWatcher] = None

    # ---------- startup/shutdown ----------

    def start(self) -> None:
        # Load configs
        self.common.loadCommonFile()
        self.peers_cfg.loadConfigFile()

        # Build peer map/list
        for pid_str, rpi in self.peers_cfg.getPeerInfoMap().items():
            pid = int(pid_str)
            self.peer_map[pid] = rpi
            self.peer_list.append(pid)
            if pid == self.my_id:
                self.my_row = rpi
        self.peer_list.sort()
        if self.my_row is None:
            raise RuntimeError(f"Peer ID {self.my_id} not found in PeerInfo.cfg")

        # Storage and pieces
        self.peer_dir = os.path.join(os.getcwd(), f"peer_{self.my_id}")
        os.makedirs(self.peer_dir, exist_ok=True)
        self.piece_mgr = PieceManager(
            peer_dir=self.peer_dir,
            file_name=self.common.FileName,
            file_size=self.common.FileSize,
            piece_size=self.common.PieceSize,
            has_full_file=(self.my_row.containsFile == 1),
        )

        # Start server (inbound neighbors)
        self._start_server()

        # Outgoing connections to earlier peers
        self._connect_to_earlier_peers()

        # Schedulers
        self._start_schedulers()

        # Termination watcher
        self._start_terminate_watcher()

    def stop(self) -> None:
        self._stop_ev.set()
        try:
            if self._opt_sched: self._opt_sched.stop()
            if self._choke_sched: self._choke_sched.stop()
            if self._term_watcher: self._term_watcher.stop()
        except Exception:
            pass
        with self._lock:
            for h in list(self._handlers.values()):
                try:
                    h._conn.close()
                except Exception:
                    pass
            self._handlers.clear()
        try:
            if self._server: self._server.stop()
        except Exception:
            pass
        try:
            self.logger.closeLogger()
        except Exception:
            pass

    # ---------- server and connections ----------

    def _start_server(self) -> None:
        assert self.my_row is not None and self.piece_mgr is not None
        # Allow uploads only to preferred or optimistic neighbors [8]
        def allows_upload_to(peer_id: int) -> bool:
            with self._lock:
                return peer_id in self._unchoked or (self._optimistic is not None and peer_id == self._optimistic)

        # Register inbound handlers after handshake learns peer_id
        def register_handler(peer_id: int, handler: PeerHandler) -> None:
            with self._lock:
                self._handlers[peer_id] = handler

        # Broadcast HAVE to other neighbors
        def broadcast_have(idx: int, origin: Optional[PeerHandler] = None) -> None:
            self._broadcast_have(idx, origin)

        self._server = PeerServer(
            host=self.my_row.peerAddress,
            port=self.my_row.peerPort,
            my_peer_id=self.my_id,
            logger=self.logger,
            piece_mgr=self.piece_mgr,
            allows_upload_to=allows_upload_to,
        )
        # If your PeerServer doesn’t already register handlers, modify it to call:
        # register_handler(peer_id, handler) once the handshake completes.
        self._server.start()

    def _connect_to_earlier_peers(self) -> None:
        assert self.piece_mgr is not None
        for pid in self.peer_list:
            if pid == self.my_id:
                break
            r = self.peer_map[pid]
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((r.peerAddress, r.peerPort))
                h = PeerHandler(
                    sock=s,
                    my_peer_id=self.my_id,
                    logger=self.logger,
                    piece_mgr=self.piece_mgr,
                    outgoing=True,
                    expected_peer_id=pid,
                    broadcast_have=lambda idx, origin=None: self._broadcast_have(idx, origin),
                    allows_upload_to=lambda nid=pid: self._allows_upload_to(nid),
                )
                with self._lock:
                    self._handlers[pid] = h
                t = threading.Thread(target=h.run, daemon=True)
                t.start()
            except Exception as e:
                print(f"[admin {self.my_id}] failed to connect to {pid} at {r.peerAddress}:{r.peerPort}: {e}")

    # ---------- scheduler wiring ----------

    def _start_schedulers(self) -> None:
        assert self.piece_mgr is not None

        def have_complete_file() -> bool:
            return self.piece_mgr.complete()

        def get_interested():
            with self._lock:
                return [pid for pid, h in self._handlers.items() if getattr(h, "_they_are_interested", False)]

        def get_download_rates():
            with self._lock:
                return {pid: getattr(h, "get_download_rate", lambda: 0)() for pid, h in self._handlers.items()}

        def get_unchoked_set() -> Set[int]:
            with self._lock:
                return set(self._unchoked)

        def set_unchoked_set(s: Set[int]) -> None:
            with self._lock:
                self._unchoked = set(s)

        def get_opt() -> Optional[int]:
            with self._lock:
                return self._optimistic

        def set_opt(pid: Optional[int]) -> None:
            with self._lock:
                self._optimistic = pid

        def get_handler(pid: int):
            with self._lock:
                return self._handlers.get(pid)

        def all_done() -> bool:
            # Conservative: declare done when all known peers (including us) report complete bitfields [8]
            if not self.piece_mgr.complete():
                return False
            with self._lock:
                # If you wire neighbor bitfield callbacks, check neighbor completeness here
                return all(True for _ in self._handlers.keys())

        self._choke_sched = ChokeScheduler(
            k=self.common.NumberOfPreferredNeighbors,
            interval_sec=self.common.UnchokingInterval,
            have_complete_file=have_complete_file,
            get_interested=get_interested,
            get_download_rates=get_download_rates,
            get_unchoked_set=get_unchoked_set,
            set_unchoked_set=set_unchoked_set,
            get_optimistic_unchoked=get_opt,
            get_handler=get_handler,
            logger=self.logger,
            on_all_done=all_done,
        )
        self._opt_sched = OptimisticUnchokeScheduler(
            interval_sec=self.common.OptimisticUnchokingInterval,
            get_interested=get_interested,
            get_unchoked_set=get_unchoked_set,
            get_optimistic_unchoked=get_opt,
            set_optimistic_unchoked=set_opt,
            get_handler=get_handler,
            logger=self.logger,
            on_all_done=all_done,
        )
        self._choke_sched.start()
        self._opt_sched.start()

    def _start_terminate_watcher(self) -> None:
        assert self.piece_mgr is not None

        def is_done():
            # Same condition as in _start_schedulers
            if not self.piece_mgr.complete():
                return False
            with self._lock:
                return all(True for _ in self._handlers.keys())

        def on_done():
            try:
                self.stop()
            except Exception:
                pass

        self._term_watcher = TerminateWatcher(poll_sec=self.common.UnchokingInterval * 2 or 10,
                                              is_done=is_done, on_done=on_done)
        self._term_watcher.start()

    # ---------- helpers ----------

    def _broadcast_have(self, piece_index: int, origin: Optional[PeerHandler] = None) -> None:
        with self._lock:
            for pid, h in self._handlers.items():
                if h is origin:
                    continue
                try:
                    h.send_have(piece_index)
                except Exception:
                    pass

    def _allows_upload_to(self, peer_id: int) -> bool:
        with self._lock:
            return peer_id in self._unchoked or (self._optimistic is not None and peer_id == self._optimistic)


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python peer_admin.py <peerID>")
        sys.exit(1)
    admin = PeerAdmin(int(sys.argv[1]))
    admin.start()
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        admin.stop()