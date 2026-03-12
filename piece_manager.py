# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 16:06:40 2026

@author: Sofie
"""

# piece_manager.py
from __future__ import annotations

import os
import random
from dataclasses import dataclass
from typing import Iterable, Optional


@dataclass(frozen=True)
class PieceRef:
    index: int
    offset: int
    length: int


class Bitfield:
    """
    Bitfield encoding per project spec:
      - bit i corresponds to piece index i
      - within each byte: indices 0..7 map high-bit..low-bit [1]
      - spare bits at end must be 0 [1]
    """
    def __init__(self, num_pieces: int, initial_have_all: bool = False):
        if num_pieces < 0:
            raise ValueError("num_pieces must be non-negative")
        self.num_pieces = num_pieces
        self._bytes = bytearray((num_pieces + 7) // 8)

        if initial_have_all:
            for i in range(num_pieces):
                self.set(i, True)
            self._clear_spare_bits()

    @classmethod
    def from_bytes(cls, num_pieces: int, data: bytes) -> "Bitfield":
        bf = cls(num_pieces, initial_have_all=False)
        expected_len = (num_pieces + 7) // 8
        if len(data) != expected_len:
            raise ValueError(f"Bad bitfield length: got {len(data)}, expected {expected_len}")
        bf._bytes[:] = data
        bf._validate_spare_bits_zero()
        return bf

    def to_bytes(self) -> bytes:
        self._clear_spare_bits()
        return bytes(self._bytes)

    def has(self, index: int) -> bool:
        self._check_index(index)
        byte_i = index // 8
        bit_i = 7 - (index % 8)  # high-to-low [1]
        return (self._bytes[byte_i] >> bit_i) & 1 == 1

    def set(self, index: int, value: bool = True) -> None:
        self._check_index(index)
        byte_i = index // 8
        bit_i = 7 - (index % 8)  # high-to-low [1]
        mask = 1 << bit_i
        if value:
            self._bytes[byte_i] |= mask
        else:
            self._bytes[byte_i] &= (~mask) & 0xFF

    def count_have(self) -> int:
        # Count only real pieces (ignore spare bits)
        return sum(1 for i in range(self.num_pieces) if self.has(i))

    def complete(self) -> bool:
        return self.count_have() == self.num_pieces

    def interesting_pieces(self, other: "Bitfield") -> list[int]:
        """
        Return indices that other has and we don't have.
        """
        if self.num_pieces != other.num_pieces:
            raise ValueError("bitfields must have same num_pieces")
        return [i for i in range(self.num_pieces) if other.has(i) and not self.has(i)]

    def is_interested_in(self, other: "Bitfield") -> bool:
        """
        True if other has any piece we don't have [1].
        """
        return any(other.has(i) and not self.has(i) for i in range(self.num_pieces))

    def _check_index(self, index: int) -> None:
        if index < 0 or index >= self.num_pieces:
            raise IndexError("piece index out of range")

    def _clear_spare_bits(self) -> None:
        spare = (8 - (self.num_pieces % 8)) % 8
        if spare == 0 or len(self._bytes) == 0:
            return
        keep = 0xFF & (~((1 << spare) - 1))  # keep top bits, clear bottom spare bits [1]
        self._bytes[-1] &= keep

    def _validate_spare_bits_zero(self) -> None:
        spare = (8 - (self.num_pieces % 8)) % 8
        if spare == 0 or len(self._bytes) == 0:
            return
        # If spare bits are low bits, they must be zero [1]
        mask = (1 << spare) - 1
        if (self._bytes[-1] & mask) != 0:
            raise ValueError("spare bits in bitfield must be zero")

    def __repr__(self) -> str:
        return f"Bitfield(num_pieces={self.num_pieces}, have={self.count_have()})"


class PieceManager:
    """
    Project piece storage + bitfield:
      - pieces are whole pieces (no subpieces) [1]
      - file is stored under peer_dir/file_name [1]
    """
    def __init__(
        self,
        *,
        peer_dir: str,
        file_name: str,
        file_size: int,
        piece_size: int,
        has_full_file: bool,
    ):
        self.peer_dir = peer_dir
        self.file_name = file_name
        self.file_size = file_size
        self.piece_size = piece_size

        os.makedirs(self.peer_dir, exist_ok=True)
        self.path = os.path.join(self.peer_dir, self.file_name)

        self.num_pieces = (file_size + piece_size - 1) // piece_size
        self.bitfield = Bitfield(self.num_pieces, initial_have_all=has_full_file)

        # Ensure the backing file exists with correct size (for random-access writes)
        if not os.path.exists(self.path):
            with open(self.path, "wb") as f:
                f.truncate(self.file_size)
        else:
            # If seeder, file should already exist per spec [1]; still safe to verify size.
            with open(self.path, "ab") as f:
                pass

        # Track requested pieces to satisfy “not requested from other neighbors” [1]
        self._requested = set()  # piece indices currently requested from someone

    # -------- Piece layout --------

    def piece_ref(self, index: int) -> PieceRef:
        if index < 0 or index >= self.num_pieces:
            raise IndexError("piece index out of range")
        offset = index * self.piece_size
        length = min(self.piece_size, self.file_size - offset)
        return PieceRef(index=index, offset=offset, length=length)

    # -------- Disk I/O --------

    def read_piece(self, index: int) -> bytes:
        """
        Used when we receive REQUEST and want to send PIECE [1].
        """
        if not self.bitfield.has(index):
            raise ValueError("Cannot read a piece we do not have")
        ref = self.piece_ref(index)
        with open(self.path, "rb") as f:
            f.seek(ref.offset)
            data = f.read(ref.length)
        if len(data) != ref.length:
            raise IOError("Short read while reading piece")
        return data

    def write_piece(self, index: int, data: bytes) -> bool:
        """
        Used when we receive PIECE and want to persist + update bitfield [1].
        Returns True if this call newly completed the piece (i.e., it was not owned before).
        """
        ref = self.piece_ref(index)
        if len(data) != ref.length:
            raise ValueError(f"Piece length mismatch: expected {ref.length}, got {len(data)}")

        already_had = self.bitfield.has(index)
        if not already_had:
            with open(self.path, "r+b") as f:
                f.seek(ref.offset)
                f.write(data)
            self.bitfield.set(index, True)

        # once received (or if it was already present), it should no longer be "requested"
        self._requested.discard(index)
        return (not already_had)

    # -------- Request bookkeeping --------

    def mark_requested(self, index: int) -> None:
        self._requested.add(index)

    def unmark_requested(self, index: int) -> None:
        self._requested.discard(index)

    def is_requested(self, index: int) -> bool:
        return index in self._requested

    def choose_random_requestable_piece(self, neighbor_bitfield: Bitfield) -> Optional[int]:
        """
        Randomly choose a piece that:
          - neighbor has
          - we don't have
          - we haven't requested yet (from any neighbor) [1]
        Returns None if nothing qualifies.
        """
        candidates = [
            i for i in range(self.num_pieces)
            if neighbor_bitfield.has(i) and not self.bitfield.has(i) and (i not in self._requested)
        ]
        if not candidates:
            return None
        return random.choice(candidates)

    # -------- Completion --------

    def complete(self) -> bool:
        return self.bitfield.complete()