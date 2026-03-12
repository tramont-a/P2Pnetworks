# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 20:12:46 2026

@author: Sofie
"""
import socket
import struct
from dataclasses import dataclass
from typing import Optional

HANDSHAKE_HEADER = b"P2PFILESHARINGPROJ"  # 18 bytes [1]
HANDSHAKE_LEN = 32                       # [1]
ZERO_BITS_LEN = 10                       # [1]

# Message types [1]
CHOKE = 0
UNCHOKE = 1
INTERESTED = 2
NOT_INTERESTED = 3
HAVE = 4
BITFIELD = 5
REQUEST = 6
PIECE = 7


@dataclass(frozen=True)
class PeerMessage:
    msg_type: int
    payload: bytes = b""


class PeerConnection:
    """
    Stream-safe TCP peer connection:
      - handshake: 32 bytes (header + zero bits + peer id) [1]
      - messages: 4-byte length (excluding itself) + 1-byte type + payload [1]
    """

    def __init__(self, sock: socket.socket):
        self.sock = sock
        # Keep it simple and reliable for a byte-stream: use blocking mode.
        self.sock.setblocking(True)

    # ---------------- Handshake ----------------

    def send_handshake(self, my_peer_id: int) -> None:
        msg = self._build_handshake(my_peer_id)
        self._sendall(msg)

    def recv_and_validate_handshake(self, expected_peer_id: Optional[int] = None) -> int:
        data = self._recvall(HANDSHAKE_LEN)

        header = data[:18]
        if header != HANDSHAKE_HEADER:
            raise IOError(f"Bad handshake header: {header!r}")

        zeros = data[18:18 + ZERO_BITS_LEN]
        if zeros != b"\x00" * ZERO_BITS_LEN:
            raise IOError("Bad handshake zero bits")

        (peer_id,) = struct.unpack(">I", data[28:32])  # 4-byte peer id [1]
        if expected_peer_id is not None and peer_id != expected_peer_id:
            raise IOError(f"Unexpected peer id {peer_id} (expected {expected_peer_id})")
        return peer_id

    @staticmethod
    def _build_handshake(peer_id: int) -> bytes:
        if len(HANDSHAKE_HEADER) != 18:
            raise ValueError("Handshake header must be 18 bytes")
        return HANDSHAKE_HEADER + (b"\x00" * ZERO_BITS_LEN) + struct.pack(">I", peer_id)

    # ---------------- Message Framing ----------------
    # actual message: <len:4><type:1><payload...> [1]
    # len excludes the 4-byte length field itself [1]

    def send_message(self, msg_type: int, payload: bytes = b"") -> None:
        if payload is None:
            payload = b""
        length = 1 + len(payload)  # type + payload [1]
        frame = struct.pack(">I", length) + struct.pack(">B", msg_type) + payload
        self._sendall(frame)

    def recv_message(self) -> PeerMessage:
        (length,) = struct.unpack(">I", self._recvall(4))
        if length < 1:
            raise IOError(f"Invalid message length {length}")

        (msg_type,) = struct.unpack(">B", self._recvall(1))
        payload_len = length - 1
        payload = self._recvall(payload_len) if payload_len else b""
        return PeerMessage(msg_type=msg_type, payload=payload)

    # ---------------- Helpers for project payloads ----------------

    def send_choke(self) -> None:
        self.send_message(CHOKE)

    def send_unchoke(self) -> None:
        self.send_message(UNCHOKE)

    def send_interested(self) -> None:
        self.send_message(INTERESTED)

    def send_not_interested(self) -> None:
        self.send_message(NOT_INTERESTED)

    def send_have(self, piece_index: int) -> None:
        # payload: 4-byte piece index [1]
        self.send_message(HAVE, struct.pack(">I", piece_index))

    def send_request(self, piece_index: int) -> None:
        # payload: 4-byte piece index (no subpieces) [1]
        self.send_message(REQUEST, struct.pack(">I", piece_index))

    def send_piece(self, piece_index: int, piece_bytes: bytes) -> None:
        # payload: 4-byte piece index + piece content [1]
        self.send_message(PIECE, struct.pack(">I", piece_index) + piece_bytes)

    @staticmethod
    def parse_have_payload(payload: bytes) -> int:
        if len(payload) != 4:
            raise ValueError("HAVE payload must be 4 bytes")
        (idx,) = struct.unpack(">I", payload)
        return idx

    @staticmethod
    def parse_request_payload(payload: bytes) -> int:
        if len(payload) != 4:
            raise ValueError("REQUEST payload must be 4 bytes")
        (idx,) = struct.unpack(">I", payload)
        return idx

    @staticmethod
    def parse_piece_payload(payload: bytes) -> tuple[int, bytes]:
        if len(payload) < 4:
            raise ValueError("PIECE payload must be at least 4 bytes")
        (idx,) = struct.unpack(">I", payload[:4])
        return idx, payload[4:]

    # ---------------- Stream primitives ----------------

    def _sendall(self, data: bytes) -> None:
        view = memoryview(data)
        while view:
            sent = self.sock.send(view)
            if sent <= 0:
                raise IOError("Socket closed while sending")
            view = view[sent:]

    def _recvall(self, n: int) -> bytes:
        chunks = []
        remaining = n
        while remaining > 0:
            chunk = self.sock.recv(remaining)
            if not chunk:
                raise IOError("Socket closed while receiving")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def close(self) -> None:
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
