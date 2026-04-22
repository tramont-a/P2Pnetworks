#!/usr/bin/env python3
import socket
import threading
import time
import sys
import os
from typing import Dict, List, Optional, Set

from peer2peerconnect import (
    PeerConnection,
    CHOKE,
    UNCHOKE,
    INTERESTED,
    NOT_INTERESTED,
    HAVE,
    BITFIELD,
    REQUEST,
    PIECE,
)
from piece_manager import PieceManager, Bitfield
from peer_logging import peerLogger as logger # your logging class [3]

# ---------------------- Config parsing ---------------------- [1][2][10]

class CommonConfig:
    def __init__(self, path: str):
        self.path = path
        self.num_pref_neighbors = 0
        self.unchoke_interval = 0
        self.opt_unchoke_interval = 0
        self.file_name = ""
        self.file_size = 0
        self.piece_size = 0
        self._parse()

    def _parse(self) -> None:
        with open(self.path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                key, value = line.split()
                if key == "NumberOfPreferredNeighbors":
                    self.num_pref_neighbors = int(value)
                elif key == "UnchokingInterval":
                    self.unchoke_interval = int(value)
                elif key == "OptimisticUnchokingInterval":
                    self.opt_unchoke_interval = int(value)
                elif key == "FileName":
                    self.file_name = value
                elif key == "FileSize":
                    self.file_size = int(value)
                elif key == "PieceSize":
                    self.piece_size = int(value)


class PeerInfoEntry:
    def __init__(self, peer_id: int, host: str, port: int, has_file: bool):
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.has_file = has_file
        


def parse_peerinfo(path: str) -> List[PeerInfoEntry]:
    #print("Parsing peer info.")
    peers: List[PeerInfoEntry] = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            pid_str, host, port_str, has_str = line.split()
            peers.append(
                PeerInfoEntry(
                    peer_id=int(pid_str),
                    host=host,
                    port=int(port_str),
                    has_file=(has_str == "1"),
                )
            )
            #print(pid_str)
    #print(peers)
    return peers


# ---------------------- Peer state structures ----------------------

class NeighborState:
    """
    Manages per-neighbor state: bitfield, choke/interest flags, and download rate.
    """

    def __init__(self, peer_id: int, conn: PeerConnection, num_pieces: int):
        self.peer_id = peer_id
        self.conn = conn
        self.bitfield = Bitfield(num_pieces, initial_have_all=False)
        self.choked_by_us = True     # we initially choke everyone
        self.choked_us = True        # they have not unchoked us yet
        self.interested_in_us = False
        self.we_are_interested = False
        # bytes downloaded from this neighbor during current interval
        self.bytes_downloaded_interval = 0.0


# ---------------------- PeerProcess core ---------------------- [10]

class PeerProcess:
    def __init__(self, my_peer_id: int):
        self.my_peer_id = my_peer_id

        # read configs [1][2][10]
        self.common = CommonConfig("Common.cfg")
        self.peers = parse_peerinfo("PeerInfo.cfg")

        # find self entry [10]
        my_entry = next(p for p in self.peers if p.peer_id == self.my_peer_id)
        self.my_host = my_entry.host
        self.my_port = my_entry.port
        self.have_full_file_at_start = my_entry.has_file

        # piece manager: uses ./peer_[peerID] subdir [6][10]
        self.pm = PieceManager(
            peer_dir=f"peer_{self.my_peer_id}",
            file_name=self.common.file_name,
            file_size=self.common.file_size,
            piece_size=self.common.piece_size,
            has_full_file=self.have_full_file_at_start,
        )

        # logging [3][10]
        self.logger = logger()

        # neighbors
        self.neighbors_lock = threading.Lock()
        self.neighbors: Dict[int, NeighborState] = {}  # peer_id -> state

        # threading
        self.first_neighbor_connected = threading.Event()
        
        # sets for unchoking
        self.preferred_neighbors: Set[int] = set()
        self.optimistic_unchoke: Optional[int] = None

        # termination
        self.all_peers_complete = False
        self.start_time = time.time()

    # ---------------------- Connection management ----------------------
    def _handle_peer_disconnect(self, peer_id: int, reason: str) -> None:
        """Handle peer disconnection and cleanup"""
        #print(f"Handling disconnect for peer {peer_id}: {reason}")
        
        with self.neighbors_lock:
            if peer_id not in self.neighbors:
                return
            
            ns = self.neighbors[peer_id]
            
            # Close the connection safely
            try:
                ns.conn.close()
            except Exception:
                pass
            
            # Remove from neighbors
            del self.neighbors[peer_id]
            
            # Remove from preferred/optimistic sets
            self.preferred_neighbors.discard(peer_id)
            if self.optimistic_unchoke == peer_id:
                self.optimistic_unchoke = None
        
        # Log the disconnection
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"{now}: Peer {self.my_peer_id} lost connection to Peer {peer_id}")
    
    def start(self) -> None:
        """
        Entry point: start server listener and outgoing connections,
        then start timers and wait until termination condition. [10]
        """
        print("Starting up connection...")

        # Start server thread (accept connections from later peers) [10]
        server_thread = threading.Thread(target=self._server_loop, daemon=True)
        server_thread.start()

        # Make outgoing connections to peers listed before us [10]
        #print("Try connecting to earlier peers...")
        self._connect_to_earlier_peers()
       # print("Done? connecting to earlier peers.")

        if not self.first_neighbor_connected.wait(timeout=30.0):
            print("Timeout.")

        # Start choking/unchoking timers [10]
        threading.Thread(target=self._unchoke_timer_loop, daemon=True).start()
        threading.Thread(target=self._optimistic_unchoke_timer_loop, daemon=True).start()

        # Main completion loop: periodically check if all peers are complete [10]
        while not self.all_peers_complete:
            # we know our own completion from PieceManager
            if self.pm.complete():
                # log our completion if just finished [3][10]
                # (PieceManager.write_piece caller should also log pieceDL and completion)
                pass
            # In this simple skeleton, we assume that when *we* have complete file
            # and see, via neighbor bitfields/have messages, that all neighbors have it,
            # we can exit. [10]
            if self._everyone_complete():
                self.all_peers_complete = True
                break
            time.sleep(1.0)

        # Program ends
        # You may close sockets explicitly or rely on OS cleanup.

    def _server_loop(self) -> None:
        """Listen for incoming TCP connections and spawn handlers. [10]"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", self.my_port))
        sock.listen()
        while True:
            conn_sock, addr = sock.accept()
            threading.Thread(
                target=self._handle_incoming_connection, args=(conn_sock,), daemon=True
            ).start()

    def _handle_incoming_connection(self, conn_sock: socket.socket) -> None:
        pc = PeerConnection(conn_sock)
        try:
            # Receive handshake from remote [10]
            remote_id = pc.recv_and_validate_handshake()

            if remote_id == self.my_peer_id:
                #print(f"Rejecting self-connection: {remote_id}")
                pc.close()
                return
            # Send our handshake [10]
            pc.send_handshake(self.my_peer_id)

            # Log connection:
            # remote peer made connection to us [3][10]
            now = time.strftime("%Y-%m-%d %H:%M:%S")
            # ID1 = logger (us), ID2 = connector (remote) [3]
            self.logger.estConnection(str(self.my_peer_id), str(remote_id), now)

            # Register neighbor
            self._register_neighbor(remote_id, pc)

            # Send our bitfield if we have any piece [10]
            if self.pm.bitfield.count_have() > 0:
                bitfield_bytes = self.pm.bitfield.to_bytes()
                pc.send_bitfield(bitfield_bytes)

            # Spawn message loop
            self._connection_message_loop(remote_id)
        except Exception as e:
            # For debugging: print or log error
            pc.close()

    def _connect_to_earlier_peers(self) -> None:
        #print("Connect to earlier peers.")
        my_index = next(i for i, p in enumerate(self.peers) if p.peer_id == self.my_peer_id)
        if my_index == 0:
            #print("No earlier peers to connect to.")
            return
        
        earlier = self.peers[:my_index]
        #print(f"Earlier peers to connect to: {[(p.peer_id, p.host, p.port) for p in earlier]}")  # Debug step 3

        for p in earlier:
            if p.peer_id == self.my_peer_id:
                continue
                
            #print(f"Attempting to connect to peer {p.peer_id} at {p.host}:{p.port}")  # Debug step 4
            max_attempts = 5  # Add retry limit instead of infinite loop
            attempt = 0
            connected = False
            
            while attempt < max_attempts and not connected:
                try:
                    print(f"Connection attempt {attempt + 1} to peer {p.peer_id}")  # Debug step 5
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(5.0)  # Add socket timeout

                    s.connect((p.host, p.port))
                    print(f"✓ Socket connected to peer {p.peer_id}")  # Debug step 6
                    pc = PeerConnection(s)
                    
                    print(f"Sending handshake to peer {p.peer_id}")  # Debug step 7
                    pc.send_handshake(self.my_peer_id)
                    
                    print(f"Receiving handshake from peer {p.peer_id}")  # Debug step 8
                    remote_id = pc.recv_and_validate_handshake(expected_peer_id=p.peer_id)
                    print(f"✓ Handshake complete with peer {remote_id}")  # Debug step 9
                    
                    # Register neighbor [13]
                    self._register_neighbor(remote_id, pc)

                    # Log connection [13]
                    now = time.strftime("%Y-%m-%d %H:%M:%S")
                    self.logger.estConnection(str(self.my_peer_id), str(remote_id), now)
                    
                    # Send our bitfield if we have any pieces [13]
                    if self.pm.bitfield.count_have() > 0:
                        bitfield_bytes = self.pm.bitfield.to_bytes()
                        pc.send_bitfield(bitfield_bytes)
                    
                    # Start message loop in separate thread [13]
                    threading.Thread(
                        target=self._connection_message_loop,
                        args=(remote_id,),
                        daemon=True,
                    ).start()
                    
                    print(f"✓ Successfully connected to peer {p.peer_id}")
                    time.sleep(0.5)
                    connected = True
                    break  # Connection successful
                    
                except socket.timeout:
                    attempt += 1
                    #print(f"❌ Timeout connecting to peer {p.peer_id}")
                except ConnectionRefusedError:
                    attempt += 1
                    #print(f"❌ Connection refused by peer {p.peer_id} - peer not running?")
                except Exception as e:
                    attempt += 1
                    #print(f"❌ Connection attempt {attempt} to peer {p.peer_id} failed: {type(e).__name__}: {e}")
                    if attempt < max_attempts:
                        time.sleep(1.0)
            
            if attempt >= max_attempts:
                print(f"Failed to connect to peer {p.peer_id} after {max_attempts} attempts")

    def _register_neighbor(self, remote_id: int, pc: PeerConnection) -> None:
        with self.neighbors_lock:
            if remote_id not in self.neighbors:
                self.neighbors[remote_id] = NeighborState(
                    peer_id=remote_id,
                    conn=pc,
                    num_pieces=self.pm.num_pieces,
                )
                self.first_neighbor_connected.set()

    # ---------------------- Message handling loop ---------------------- [10]

    def _connection_message_loop(self, remote_id: int) -> None:
        while True:
            with self.neighbors_lock:
                if remote_id not in self.neighbors:
                    return
                ns = self.neighbors[remote_id]
                conn = ns.conn
            
            try:
                # Set a reasonable timeout for message reception
                conn.sock.settimeout(30.0)  # 30 second timeout
                msg = conn.recv_message()
                
                # Reset timeout after successful message
                conn.sock.settimeout(None)
                
            except socket.timeout:
                #print(f"Peer {remote_id} connection timed out")
                self._handle_peer_disconnect(remote_id, "timeout")
                return
            except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
                #print(f"Peer {remote_id} connection reset by peer")
                self._handle_peer_disconnect(remote_id, "connection_reset")
                return
            except Exception as e:
                #print(f"Connection to peer {remote_id} failed: {e}")
                self._handle_peer_disconnect(remote_id, "error")
                return

            if msg.msg_type == CHOKE:
                self._handle_choke(remote_id)
            elif msg.msg_type == UNCHOKE:
                self._handle_unchoke(remote_id)
            elif msg.msg_type == INTERESTED:
                self._handle_interested(remote_id)
            elif msg.msg_type == NOT_INTERESTED:
                self._handle_not_interested(remote_id)
            elif msg.msg_type == HAVE:
                self._handle_have(remote_id, msg.payload)
            elif msg.msg_type == BITFIELD:
                self._handle_bitfield(remote_id, msg.payload)
            elif msg.msg_type == REQUEST:
                self._handle_request(remote_id, msg.payload)
            elif msg.msg_type == PIECE:
                self._handle_piece(remote_id, msg.payload)

    # ---------------------- Individual message handlers ----------------------

    def _handle_choke(self, remote_id: int) -> None:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        with self.neighbors_lock:
            ns = self.neighbors.get(remote_id)
            if ns is None:
                return
            ns.choked_us = True
        # Log "is choked by" [3][10]
        self.logger.choking(str(remote_id), str(self.my_peer_id), now)

    def _handle_unchoke(self, remote_id: int) -> None:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        with self.neighbors_lock:
            ns = self.neighbors.get(remote_id)
            if ns is None:
                return
            ns.choked_us = False
        # Log "is unchoked by" [3][10]
        self.logger.unchoking(str(remote_id), str(self.my_peer_id), now)
        # When unchoked, send first request if there is an interesting piece [10]
        self._maybe_send_request(remote_id)

    def _handle_interested(self, remote_id: int) -> None:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        with self.neighbors_lock:
            ns = self.neighbors.get(remote_id)
            if ns is None:
                return
            ns.interested_in_us = True
        # Log [3][10]
        self.logger.recInterest(str(self.my_peer_id), str(remote_id), now)

    def _handle_not_interested(self, remote_id: int) -> None:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        with self.neighbors_lock:
            ns = self.neighbors.get(remote_id)
            if ns is None:
                return
            ns.interested_in_us = False
        # Log [3][10]
        self.logger.recNotInterest(str(self.my_peer_id), str(remote_id), now)

    def _handle_have(self, remote_id: int, payload: bytes) -> None:
        idx = PeerConnection.parse_index_payload(payload)
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        with self.neighbors_lock:
            ns = self.neighbors.get(remote_id)
            if ns is None:
                return
            ns.bitfield.set(idx, True)
        # Log [3][10]
        self.logger.recHave(str(self.my_peer_id), str(remote_id), idx, now)
        # Decide if we are interested [10]
        self._update_interest_for_neighbor(remote_id)

    def _handle_bitfield(self, remote_id: int, payload: bytes) -> None:
        with self.neighbors_lock:
            ns = self.neighbors.get(remote_id)
            if ns is None:
                return
            ns.bitfield = Bitfield.from_bytes(self.pm.num_pieces, payload)
        # After receiving bitfield, decide interested/not interested and send message [10]
        self._update_interest_for_neighbor(remote_id)

    def _handle_request(self, remote_id: int, payload: bytes) -> None:
        # Only serve if this neighbor is currently unchoked by us [10]
        with self.neighbors_lock:
            ns = self.neighbors.get(remote_id)
            if ns is None or ns.choked_by_us:
                return
            conn = ns.conn
        idx = PeerConnection.parse_index_payload(payload)
        try:
            data = self.pm.read_piece(idx)
        except Exception:
            return
        conn.send_piece(idx, data)

    def _handle_piece(self, remote_id: int, payload: bytes) -> None:
        idx, data = PeerConnection.parse_piece_payload(payload)
        new_piece = self.pm.write_piece(idx, data)
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        # Update download stats for rate calculation [10]
        with self.neighbors_lock:
            ns = self.neighbors.get(remote_id)
            if ns is not None:
                ns.bytes_downloaded_interval += len(data)

        if new_piece:
            # Log piece download [3][10]
            num_have = self.pm.bitfield.count_have()
            self.logger.pieceDL(str(self.my_peer_id), str(remote_id), idx, num_have, now)
            # If we just completed the file, log completion [3][10]
            if self.pm.complete():
                self.logger.completeDL(str(self.my_peer_id), now)

            # Send HAVE to all neighbors [10]
            with self.neighbors_lock:
                for pid, ns2 in self.neighbors.items():
                    ns2.conn.send_have(idx)

        # After receiving a piece, send the next REQUEST if still unchoked and interested [10]
        self._maybe_send_request(remote_id)

    # ---------------------- Interest and request selection ---------------------- [10][6]

    def _update_interest_for_neighbor(self, remote_id: int) -> None:
        with self.neighbors_lock:
            ns = self.neighbors.get(remote_id)
            if ns is None:
                return
            interested = self.pm.bitfield.is_interested_in(ns.bitfield)
            conn = ns.conn
        if interested and not ns.we_are_interested:
            conn.send_interested()
            ns.we_are_interested = True
        elif not interested and ns.we_are_interested:
            conn.send_not_interested()
            ns.we_are_interested = False

    def _maybe_send_request(self, remote_id: int) -> None:
        with self.neighbors_lock:
            ns = self.neighbors.get(remote_id)
            if ns is None:
                return
            if ns.choked_us:
                return
            if not self.pm.bitfield.is_interested_in(ns.bitfield):
                return
            conn = ns.conn

        # Choose a random piece that neighbor has, we don't, and not previously requested [6][10]
        piece_index = self.pm.choose_random_requestable_piece(ns.bitfield)
        if piece_index is None:
            return
        self.pm.mark_requested(piece_index)
        conn.send_request(piece_index)

    # ---------------------- Choking / unchoking logic ---------------------- [10]

    def _unchoke_timer_loop(self) -> None:
        while not self.all_peers_complete:
            time.sleep(self.common.unchoke_interval)
            self._reselect_preferred_neighbors()

    def _optimistic_unchoke_timer_loop(self) -> None:
        while not self.all_peers_complete:
            time.sleep(self.common.opt_unchoke_interval)
            self._reselect_optimistic_unchoke()

    def _reselect_preferred_neighbors(self) -> None:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        with self.neighbors_lock:
            # Choose among neighbors that are interested in us [10]
            candidates = [ns for ns in self.neighbors.values() if ns.interested_in_us]

            if not self.have_full_file_at_start and not self.pm.complete():
                # Sort by download rate (bytes per interval) descending [10]
                candidates.sort(
                    key=lambda ns: ns.bytes_downloaded_interval, reverse=True
                )
            else:
                # When we have complete file, select randomly later; here just shuffle
                import random
                random.shuffle(candidates)

            # Pick top k [1][10]
            k = self.common.num_pref_neighbors
            new_pref_ids = {ns.peer_id for ns in candidates[:k]}

            # Reset stats each interval [10]
            for ns in self.neighbors.values():
                ns.bytes_downloaded_interval = 0.0

            # Send choke/unchoke updates
            for pid, ns in self.neighbors.items():
                was_pref = pid in self.preferred_neighbors
                now_pref = pid in new_pref_ids
                if now_pref and ns.choked_by_us:
                    ns.conn.send_unchoke()
                    ns.choked_by_us = False
                elif (not now_pref) and (pid != self.optimistic_unchoke) and (not ns.choked_by_us):
                    ns.conn.send_choke()
                    ns.choked_by_us = True

            self.preferred_neighbors = new_pref_ids

            # Log change of preferred neighbors [3][10]
            self.logger.changePrefNeighbor(
                str(self.my_peer_id),
                list(sorted(self.preferred_neighbors)),
                now,
            )

    def _reselect_optimistic_unchoke(self) -> None:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        with self.neighbors_lock:
            import random
            # Candidates: currently choked by us but interested in us [10]
            candidates = [
                ns.peer_id
                for ns in self.neighbors.values()
                if ns.choked_by_us and ns.interested_in_us
            ]
            if not candidates:
                return
            new_optimistic = random.choice(candidates)
            # Unchoke new optimistic if needed
            ns = self.neighbors[new_optimistic]
            if ns.choked_by_us:
                ns.conn.send_unchoke()
                ns.choked_by_us = False
            # Previous optimistic that is not preferred should be choked
            if self.optimistic_unchoke is not None:
                prev = self.optimistic_unchoke
                if (
                    prev in self.neighbors
                    and prev not in self.preferred_neighbors
                    and prev != new_optimistic
                ):
                    prev_ns = self.neighbors[prev]
                    if not prev_ns.choked_by_us:
                        prev_ns.conn.send_choke()
                        prev_ns.choked_by_us = True

            self.optimistic_unchoke = new_optimistic

            # Log optimistic unchoke [3][10]
            self.logger.changeOUN(
                str(self.my_peer_id),
                str(self.optimistic_unchoke),
                now,
            )

    # ---------------------- Completion condition ---------------------- [10]

    def _everyone_complete(self) -> bool:
        # Don't exit for at least 30 seconds (adjust as needed)
        if time.time() - self.start_time < 30:
            return False

        # Don't declare complete until we have reasonable connectivity
        if not self.pm.complete():
            return False
        
        with self.neighbors_lock:
            # Ensure we have some neighbors (avoid premature exit)
            if len(self.neighbors) == 0:
                return False
            
            # Check if all connected neighbors are complete
            all_neighbors_complete = all(ns.bitfield.complete() for ns in self.neighbors.values())
            
            # Additional safety: ensure we've been running for a minimum time
            # This gives other peers time to connect and exchange data
            return all_neighbors_complete


# ---------------------- Main ---------------------- [10]

def main():
    if len(sys.argv) != 2:
        print("Usage: peerProcess <peerID>")
        sys.exit(1)
    peer_id = int(sys.argv[1])

    # Ensure working dir is current, Common.cfg and PeerInfo.cfg present [10]
    if not os.path.exists("Common.cfg") or not os.path.exists("PeerInfo.cfg"):
        print("Common.cfg or PeerInfo.cfg not found in working directory")
        sys.exit(1)

    pp = PeerProcess(peer_id)
    pp.start()


if __name__ == "__main__":
    main()
