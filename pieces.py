# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 16:03:07 2026

@author: Sofie
"""

# pieces.py
import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class PieceRef:
    index: int
    offset: int
    length: int


class PieceManager:
    """
    Defines pieces (piece index -> bytes on disk) and
    maintains our bitfield.
    """

    def __init__(self, *, peer_dir: str, file_name: str, file_size: int, piece_size: int, has_full_file: bool):
        self.peer_dir = peer_dir
        self.file_name = file_name
        self.file_size = file_size
        self.piece_size = piece_size

        os.makedirs(self.peer_dir, exist_ok=True)
        self.path = os.path.join(self.peer_dir, self.file_name)

        self.num_pieces = (file_size + piece_size - 1) // piece_size

        # bitfield bytes sized to num_pieces, with spare bits at end = 0 [1]
        self.bitfield = bytearray((self.num_pieces + 7) // 8)

        if has_full_file:
            # set all piece bits to 1, then clear spare bits
            for i in range(self.num_pieces):
                self._set_piece_bit(i, True)
            self._clear_spare_bits()
        else:
            # ensure file exists to allow random-access writes later
            if not os.path.exists(self.path):
                with open(self.path, "wb") as f:
                    f.truncate(self.file_size)

        self._have_count = self._count_have()

    # -------- Piece mapping --------

    def piece_ref(self, index: int) -> PieceRef:
        if index < 0 or index >= self.num_pieces:
            raise IndexError("piece index out of range")
        offset = index * self.piece_size
        length = min(self.piece_size, self.file_size - offset)
        return PieceRef(index=index, offset=offset, length=length)

    # -------- Disk I/O --------

    def read_piece(self, index: int) -> bytes:
        """
        For serving a REQUEST: return full piece bytes for given index [1].
        """
        if not self.has_piece(index):
            raise ValueError("Cannot read piece we do not have")
        ref = self.piece_ref(index)
        with open(self.path, "rb") as f:
            f.seek(ref.offset)
            data = f.read(ref.length)
        if len(data) != ref.length:
            raise IOError("Short read for piece")
        return data

    def write_piece(self, index: int, data: bytes) -> None:
        """
        For handling a received PIECE: write to correct file offset, set bit.
        """
        ref = self.piece_ref(index)
        if len(data) != ref.length:
            raise ValueError(f"Piece length mismatch: expected {ref.length}, got {len(data)}")

        with open(self.path, "r+b") as f:
            f.seek(ref.offset)
            f.write(data)

        if not self.has_piece(index):
            self._set_piece_bit(index, True)
            self._have_count += 1

    # -------- Bitfield helpers --------

    def has_piece(self, index: int) -> bool:
        byte_i = index // 8
        bit_i = 7 - (index % 8)  # high-to-low bit order [1]
        return (self.bitfield[byte_i] >> bit_i) & 1 == 1

    def bitfield_bytes(self) -> bytes:
        return bytes(self.bitfield)

    def update_from_have(self, index: int) -> None:
        self._set_piece_bit(index, True)

    def complete(self) -> bool:
        return self._have_count == self.num_pieces

    def have_count(self) -> int:
        return self._have_count

    # -------- internal --------

    def _set_piece_bit(self, index: int, value: bool) -> None:
        byte_i = index // 8
        bit_i = 7 - (index % 8)  # high-to-low [1]
        mask = 1 << bit_i
        if value:
            self.bitfield[byte_i] |= mask
        else:
            self.bitfield[byte_i] &= (~mask) & 0xFF

    def _clear_spare_bits(self) -> None:
        spare = (8 - (self.num_pieces % 8)) % 8
        if spare == 0:
            return
        # last byte: keep top (8-spare) bits, clear bottom spare bits
        keep = 0xFF & (~((1 << spare) - 1))
        self.bitfield[-1] &= keep

    def _count_have(self) -> int:
        c = 0
        for i in range(self.num_pieces):
            if self.has_piece(i):
                c += 1
        return c