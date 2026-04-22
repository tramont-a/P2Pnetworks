# peer_handler.py
import socket
from typing import Optional, Callable

from peer2peerconnect import (
    PeerConnection, PeerMessage,
    CHOKE, UNCHOKE, INTERESTED, NOT_INTERESTED, HAVE, BITFIELD, REQUEST, PIECE
)
from peer_logging import peerLogger
from piece_manager import PieceManager, Bitfield


class PeerHandler:
    """
    Per-neighbor connection handler:
      - Performs handshake (32 bytes) and initial bitfield exchange [3][8]
      - Tracks neighbor bitfield, interest, and choke state
      - Implements CHOKE/UNCHOKE/INTERESTED/NOT_INTERESTED/HAVE/BITFIELD/REQUEST/PIECE workflows [3][8]
      - Exposes send_choke(), send_unchoke(), reset_download_rate() for schedulers
    """

    def __init__(
        self,
        *,
        sock: socket.socket,
        my_peer_id: int,
        logger: peerLogger,
        piece_mgr: PieceManager,
        outgoing: bool,
        expected_peer_id: Optional[int] = None,
        broadcast_have: Optional[Callable[[int, Optional["PeerHandler"]], None]] = None,
        allows_upload_to: Optional[Callable[[int], bool]] = None,
    ):
        self._sock = sock
        self._conn = PeerConnection(sock)
        self._my_peer_id = my_peer_id
        self._logger = logger
        self._pm = piece_mgr

        self._outgoing = outgoing
        self._expected_peer_id = expected_peer_id
        self._broadcast_have = broadcast_have or (lambda _idx, _origin=None: None)
        self._allows_upload_to = allows_upload_to or (lambda _peer_id: True)

        self._peer_id: Optional[int] = None
        self._neighbor_bf: Optional[Bitfield] = None

        self._am_unchoked = False          # remote has unchoked us
        self._they_are_interested = False  # remote sent INTERESTED
        self._in_flight_request: Optional[int] = None
        self._download_rate = 0            # pieces received from this neighbor in the last interval

        self._dead = False
        self._first_msg: Optional[PeerMessage] = None

    # Public entrypoint
    def run(self) -> None:
        try:
            self._handshake_and_initial_bitfield()
            self._message_loop()
        finally:
            try:
                self._conn.close()
            except Exception:
                pass

    # API expected by schedulers
    def send_choke(self) -> None:
        try:
            self._conn.send_choke()
        except Exception:
            self._dead = True

    def send_unchoke(self) -> None:
        try:
            self._conn.send_unchoke()
        except Exception:
            self._dead = True

    def reset_download_rate(self) -> None:
        self._download_rate = 0

    def get_download_rate(self) -> int:
        return self._download_rate

    # Allow server/admin to push HAVE to this neighbor
    def send_have(self, piece_index: int) -> None:
        try:
            self._conn.send_have(piece_index)
        except Exception:
            self._dead = True

    # ------------- internal -------------

    def _handshake_and_initial_bitfield(self) -> None:
        # Outgoing connects: send then recv; incoming: recv then send [8]
        if self._outgoing:
            self._conn.send_handshake(self._my_peer_id)
            self._peer_id = self._conn.recv_and_validate_handshake(self._expected_peer_id)
            self._logger.genTCPConnLogSender(str(self._peer_id))
        else:
            self._peer_id = self._conn.recv_and_validate_handshake(self._expected_peer_id)
            self._conn.send_handshake(self._my_peer_id)
            self._logger.genTCPConnLogReceiver(str(self._peer_id))

        # Send our bitfield if we have any pieces [8]
        if self._pm.bitfield.count_have() > 0:
            self._conn.send_bitfield(self._pm.bitfield.to_bytes())

        # Try to read neighbor's first message (often BITFIELD) without blocking forever [8]
        self._sock.settimeout(2.0)
        try:
            self._first_msg = self._conn.recv_message()
            if self._first_msg.msg_type == BITFIELD:
                self._neighbor_bf = Bitfield.from_bytes(self._pm.num_pieces, self._first_msg.payload)
                self._update_interest()
                self._first_msg = None  # consumed
        except Exception:
            self._first_msg = None
        finally:
            self._sock.settimeout(None)

    def _message_loop(self) -> None:
        # If a non-bitfield first message was peeked, process it first
        if self._first_msg is not None:
            self._dispatch(self._first_msg)
            self._first_msg = None

        while not self._dead:
            try:
                msg = self._conn.recv_message()
            except Exception:
                break
            self._dispatch(msg)

    def _dispatch(self, msg: PeerMessage) -> None:
        t = msg.msg_type
        if t == CHOKE:
            self._handle_choke()
        elif t == UNCHOKE:
            self._handle_unchoke()
        elif t == INTERESTED:
            self._handle_interested()
        elif t == NOT_INTERESTED:
            self._handle_not_interested()
        elif t == HAVE:
            self._handle_have(msg.payload)
        elif t == BITFIELD:
            self._handle_bitfield(msg.payload)
        elif t == REQUEST:
            self._handle_request(msg.payload)
        elif t == PIECE:
            self._handle_piece(msg.payload)
        else:
            # Unknown message type: ignore
            pass

    # ---- message handlers ----

    def _handle_choke(self) -> None:
        self._am_unchoked = False
        self._in_flight_request = None
        if self._peer_id is not None:
            self._logger.chokingNeighbor(str(self._peer_id))

    def _handle_unchoke(self) -> None:
        self._am_unchoked = True
        if self._peer_id is not None:
            self._logger.unchokedNeighbor(str(self._peer_id))
        self._choose_and_request()

    def _handle_interested(self) -> None:
        self._they_are_interested = True
        if self._peer_id is not None:
            self._logger.receiveInterested(str(self._peer_id))

    def _handle_not_interested(self) -> None:
        self._they_are_interested = False
        if self._peer_id is not None:
            self._logger.receiveNotInterested(str(self._peer_id))

    def _handle_have(self, payload: bytes) -> None:
        idx = PeerConnection.parse_index_payload(payload)
        if self._peer_id is not None:
            self._logger.receiveHave(str(self._peer_id), idx)
        if self._neighbor_bf is None:
            self._neighbor_bf = Bitfield(self._pm.num_pieces, initial_have_all=False)
        # Update neighbor bitfield
        try:
            self._neighbor_bf.set(idx, True)
        except Exception:
            pass
        # Reassess interest and potentially send INTERESTED/NOT_INTERESTED [8]
        self._update_interest()
        # If we are unchoked and have no in-flight request, try to request
        self._choose_and_request()

    def _handle_bitfield(self, payload: bytes) -> None:
        try:
            self._neighbor_bf = Bitfield.from_bytes(self._pm.num_pieces, payload)
        except Exception:
            self._neighbor_bf = None
        self._update_interest()
        self._choose_and_request()

    def _handle_request(self, payload: bytes) -> None:
        if self._peer_id is None:
            return
        # Only serve if policy allows upload to this neighbor (preferred or optimistic) [8]
        if not self._allows_upload_to(self._peer_id):
            return
        idx = PeerConnection.parse_index_payload(payload)
        try:
            data = self._pm.read_piece(idx)
        except Exception:
            return
        try:
            self._conn.send_piece(idx, data)
        except Exception:
            self._dead = True

    def _handle_piece(self, payload: bytes) -> None:
        idx, data = PeerConnection.parse_piece_payload(payload)
        try:
            newly_completed = self._pm.write_piece(idx, data)
        except Exception:
            return
        # Update rate for this neighbor (pieces received this interval)
        self._download_rate += 1
        # Clear in-flight marker if we were waiting on this piece
        if self._in_flight_request == idx:
            self._in_flight_request = None
        # Announce HAVE to all other neighbors
        self._broadcast_have(idx, origin=self)
        # Log piece download and decide next action
        if self._peer_id is not None and newly_completed:
            self._logger.downloadPiece(str(self._peer_id), idx, self._pm.bitfield.count_have())
        # If we now have the full file, we may become uninterested in some neighbors [8]
        if self._neighbor_bf is not None:
            # Try to request the next interesting piece if still unchoked
            if not self._choose_and_request():
                # No more interesting pieces from this neighbor
                try:
                    self._conn.send_not_interested()
                except Exception:
                    self._dead = True

    # ---- helpers ----

    def _update_interest(self) -> None:
        if self._neighbor_bf is None:
            return
        interested = self._pm.bitfield.is_interested_in(self._neighbor_bf)
        try:
            if interested:
                self._conn.send_interested()
            else:
                self._conn.send_not_interested()
        except Exception:
            self._dead = True

    def _choose_and_request(self) -> bool:
        """
        Attempt to choose and request one piece. Returns True if a request was sent.
        """
        if not self._am_unchoked or self._neighbor_bf is None or self._in_flight_request is not None:
            return False
        idx = self._pm.choose_random_requestable_piece(self._neighbor_bf)
        if idx is None:
            return False
        try:
            self._pm.mark_requested(idx)
            self._conn.send_request(idx)
            self._in_flight_request = idx
            return True
        except Exception:
            self._pm.unmark_requested(idx)
            self._dead = True
            return False