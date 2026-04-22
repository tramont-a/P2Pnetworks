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

        self.all_expected_peers = {p.peer_id for p in self.peers}
        self.completed_peers = set()  # Track which peers have reported completion

    # ---------------------- Connection management ----------------------
    def _handle_peer_disconnect(self, peer_id: int, reason: str) -> None:
        """Enhanced disconnect handler that tracks changes"""
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
            self._track_neighbor_changes(peer_id, "disconnected")  # Track the change
            
            # Remove from preferred/optimistic sets
            self.preferred_neighbors.discard(peer_id)
            if self.optimistic_unchoke == peer_id:
                self.optimistic_unchoke = None
        
        # Log the disconnection
        # now = time.strftime("%Y-%m-%d %H:%M:%S")
        # print(f"{now}: Peer {self.my_peer_id} lost connection to Peer {peer_id} ({reason})")
    
    def print_peer_info(self):
        print(f"\n---Peer Info Configuration ---")
        print(f"Peer ID: {self.my_peer_id}")
        print(f"  Host: {self.my_host}")
        print(f"  Port: {self.my_port}")
        print(f"  Has File: {self.pm.complete()}")
        print(f"  Bitfield: {self.pm.bitfield}")
        print(f"  ---")

    def print_common_config(self):
        print(f"\n--- Common Configuration ---")
        print(f"Preferred Neighbors: {self.common.num_pref_neighbors}")
        print(f"Unchoke Interval: {self.common.unchoke_interval}")
        print(f"Optimistic Unchoke Interval: {self.common.opt_unchoke_interval}")
        print(f"File Name: {self.common.file_name}")
        print(f"File Size: {self.common.file_size}")
        print(f"Piece Size: {self.common.piece_size}")

    def _broadcast_completion_if_needed(self) -> None:
        """Send our bitfield periodically to help others track global completion"""
        if self.pm.complete():
            with self.neighbors_lock:
                for pid, ns in self.neighbors.items():
                    try:
                        # Send updated bitfield to help neighbors know we're complete
                        bitfield_bytes = self.pm.bitfield.to_bytes()
                        ns.conn.send_bitfield(bitfield_bytes)
                    except Exception:
                        pass

    def start(self) -> None:
        """
        Entry point: start server listener and outgoing connections,
        then start timers and wait until termination condition. [10]
        """
        print("Starting up connection...")

        self.print_peer_info()
        self.print_common_config()
        
        # Initialize tracking variables
        self._last_neighbor_change_time = time.time()
        
        # Start server thread
        server_thread = threading.Thread(target=self._server_loop, daemon=True)
        server_thread.start()
        
        # Start keepalive thread
        keepalive_thread = threading.Thread(target=self._send_periodic_keepalive, daemon=True)
        keepalive_thread.start()
        
        # Make outgoing connections to peers listed before us
        self._connect_to_earlier_peers()
        
        if not self.first_neighbor_connected.wait(timeout=30.0):
            print("Timeout waiting for first neighbor")
        
        # Start choking/unchoking timers
        threading.Thread(target=self._unchoke_timer_loop, daemon=True).start()
        threading.Thread(target=self._optimistic_unchoke_timer_loop, daemon=True).start()
        
        # Main completion loop with enhanced completion check
        while not self.all_peers_complete:
            if self.pm.complete():
                pass  # Log completion if needed
            
            # Use enhanced completion check
            if self._enhanced_everyone_complete():
                self.all_peers_complete = True
                break
            time.sleep(2.0)  # Check more frequently
        
        print(f"Peer {self.my_peer_id} shutting down - all peers complete")

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
                
            print(f"Attempting to connect to peer {p.peer_id} at {p.host}:{p.port}")  # Debug step 4
            max_attempts = 5  # Add retry limit instead of infinite loop
            attempt = 0
            connected = False
            
            while attempt < max_attempts and not connected:
                try:
                    #print(f"Connection attempt {attempt + 1} to peer {p.peer_id}")  # Debug step 5
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(5.0)  # Add socket timeout

                    s.connect((p.host, p.port))
                    print(f"✓ Socket connected to peer {p.peer_id}")  # Debug step 6
                    pc = PeerConnection(s)
                    
                    #print(f"Sending handshake to peer {p.peer_id}")  # Debug step 7
                    pc.send_handshake(self.my_peer_id)
                    
                    #print(f"Receiving handshake from peer {p.peer_id}")  # Debug step 8
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
            
            #if attempt >= max_attempts:
                #print(f"Failed to connect to peer {p.peer_id} after {max_attempts} attempts")

    def _register_neighbor(self, remote_id: int, pc: PeerConnection) -> None:
        with self.neighbors_lock:
            if remote_id not in self.neighbors:
                self.neighbors[remote_id] = NeighborState(
                    peer_id=remote_id,
                    conn=pc,
                    num_pieces=self.pm.num_pieces,
                )
                self.first_neighbor_connected.set()
                self._track_neighbor_changes(remote_id, "connected")  # Track the change

    # ---------------------- Message handling loop ---------------------- [10]

    def _connection_message_loop(self, remote_id: int) -> None:
        """
        Enhanced message loop that prevents premature disconnections until all peers are complete.
        """
        consecutive_timeouts = 0
        max_consecutive_timeouts = 3  # Allow some tolerance for network issues
        
        while not self.all_peers_complete:
            with self.neighbors_lock:
                if remote_id not in self.neighbors:
                    return
                ns = self.neighbors[remote_id]
                conn = ns.conn
            
            try:
                # Use longer timeout to be more patient with peers
                conn.sock.settimeout(60.0)  # Increased from 30 to 60 seconds [11]
                msg = conn.recv_message()
                conn.sock.settimeout(None)
                
                # Reset timeout counter on successful message
                consecutive_timeouts = 0
                
                # Handle the message normally
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
                
            except socket.timeout:
                consecutive_timeouts += 1
                
                # Only disconnect on timeout if we've confirmed global completion
                # OR if we've had too many consecutive timeouts
                if self._everyone_complete():
                    #print(f"Peer {remote_id} timeout - global completion confirmed, disconnecting")
                    self._handle_peer_disconnect(remote_id, "timeout_after_completion")
                    return
                elif consecutive_timeouts >= max_consecutive_timeouts:
                    #print(f"Peer {remote_id} - too many consecutive timeouts ({consecutive_timeouts})")
                    self._handle_peer_disconnect(remote_id, "excessive_timeouts")
                    return
                else:
                    # Log but continue trying
                    #print(f"Peer {remote_id} timeout ({consecutive_timeouts}/{max_consecutive_timeouts}) - continuing...")
                    continue
                    
            except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as e:
                # For connection errors, only disconnect if global completion is confirmed
                if self._everyone_complete():
                    #print(f"Peer {remote_id} connection error after completion: {e}")
                    self._handle_peer_disconnect(remote_id, f"connection_error_after_completion: {type(e).__name__}")
                    return
                else:
                    # Try to reconnect or wait before giving up
                    #(f"Peer {remote_id} connection error before completion: {e} - attempting recovery...")
                    time.sleep(2.0)  # Brief pause before retry
                    
                    # Attempt to re-establish connection
                    if self._attempt_reconnection(remote_id):
                        #print(f"Successfully reconnected to peer {remote_id}")
                        continue
                    else:
                        #print(f"Failed to reconnect to peer {remote_id}")
                        self._handle_peer_disconnect(remote_id, f"connection_lost: {type(e).__name__}")
                        return
                        
            except Exception as e:
                # For other errors, be more conservative
                #print(f"Unexpected error with peer {remote_id}: {type(e).__name__}: {e}")
                
                # Only disconnect for unexpected errors if we're confident about global completion
                if self._everyone_complete() and time.time() - self.start_time > 60:
                    self._handle_peer_disconnect(remote_id, f"unexpected_error: {type(e).__name__}")
                    return
                else:
                    # Log and continue for now
                    time.sleep(1.0)
                    continue

    def _attempt_reconnection(self, remote_id: int) -> bool:
        """
        Attempt to reconnect to a peer that we've lost connection with.
        Returns True if reconnection successful, False otherwise.
        """
        try:
            # Find the peer info
            peer_entry = next(p for p in self.peers if p.peer_id == remote_id)
            
            # Create new socket and connection
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10.0)  # Short timeout for reconnection
            s.connect((peer_entry.host, peer_entry.port))
            
            pc = PeerConnection(s)
            
            # Perform handshake
            pc.send_handshake(self.my_peer_id)
            confirmed_id = pc.recv_and_validate_handshake(expected_peer_id=remote_id)
            
            if confirmed_id != remote_id:
                s.close()
                return False
            
            # Update the connection in our neighbor state
            with self.neighbors_lock:
                if remote_id in self.neighbors:
                    # Close old connection
                    try:
                        self.neighbors[remote_id].conn.close()
                    except Exception:
                        pass
                    
                    # Update with new connection
                    self.neighbors[remote_id].conn = pc
                    
                    # Send our current bitfield
                    if self.pm.bitfield.count_have() > 0:
                        bitfield_bytes = self.pm.bitfield.to_bytes()
                        pc.send_bitfield(bitfield_bytes)
                    
                    return True
            
            # If we get here, the neighbor was removed while we were reconnecting
            s.close()
            return False
            
        except Exception as e:
            #print(f"Reconnection to peer {remote_id} failed: {e}")
            return False

    def _send_periodic_keepalive(self) -> None:
        """
        Send periodic messages to keep connections alive and help track completion status.
        This runs in a separate thread.
        """
        while not self.all_peers_complete:
            time.sleep(30.0)  # Send keepalive every 30 seconds
            
            # Send our current bitfield to all neighbors to help them track our progress
            if self.pm.bitfield.count_have() > 0:
                bitfield_bytes = self.pm.bitfield.to_bytes()
                
                with self.neighbors_lock:
                    for remote_id, ns in list(self.neighbors.items()):
                        try:
                            ns.conn.send_bitfield(bitfield_bytes)
                        except Exception:
                            # Don't disconnect here, let the main message loop handle it
                            pass

    def _enhanced_everyone_complete(self) -> bool:
        """
        Enhanced completion check that's more conservative about declaring completion.
        """
        # Basic requirements
        if time.time() - self.start_time < 60:  # Minimum runtime
            return False
        
        if not self.pm.complete():
            return False
        
        with self.neighbors_lock:
            # Must have some neighbors to be confident
            if len(self.neighbors) == 0:
                return False
            
            # All connected neighbors must be complete
            all_neighbors_complete = all(ns.bitfield.complete() for ns in self.neighbors.values())
            
            if not all_neighbors_complete:
                return False
            
            # Additional check: have we been stable for a while?
            # This prevents premature exit if peers are still joining/leaving
            if hasattr(self, '_last_neighbor_change_time'):
                if time.time() - self._last_neighbor_change_time < 30:
                    return False
            
            # If we've had the same set of complete neighbors for a while, we can exit
            current_neighbor_set = set(self.neighbors.keys())
            if hasattr(self, '_stable_neighbor_set'):
                if self._stable_neighbor_set != current_neighbor_set:
                    self._stable_neighbor_set = current_neighbor_set
                    self._stable_since = time.time()
                    return False
                elif time.time() - self._stable_since > 20:  # Stable for 20 seconds
                    return True
            else:
                self._stable_neighbor_set = current_neighbor_set
                self._stable_since = time.time()
                return False
            
            return all_neighbors_complete
        
    def _track_neighbor_changes(self, remote_id: int, action: str) -> None:
        """
        Track when neighbors connect/disconnect to help with completion detection.
        """
        self._last_neighbor_change_time = time.time()
        # print(f"Neighbor change: peer {remote_id} {action} at {time.strftime('%H:%M:%S')}")

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
        self.logger.recHave(str(self.my_peer_id), str(remote_id), idx, now, str(self.pm.bitfield))
        # Decide if we are interested [10]
        self._update_interest_for_neighbor(remote_id)

        with self.neighbors_lock:
            ns = self.neighbors.get(remote_id)
            if ns is not None and ns.bitfield.complete():
                self.completed_peers.add(remote_id)

    def _handle_bitfield(self, remote_id: int, payload: bytes) -> None:
        with self.neighbors_lock:
            ns = self.neighbors.get(remote_id)
            if ns is None:
                return
            ns.bitfield = Bitfield.from_bytes(self.pm.num_pieces, payload)
        # After receiving bitfield, decide interested/not interested and send message [10]
        self._update_interest_for_neighbor(remote_id)
        # Check if this peer is complete
        with self.neighbors_lock:
            ns = self.neighbors.get(remote_id)
            if ns is not None and ns.bitfield.complete():
                self.completed_peers.add(remote_id)

    def _handle_request(self, remote_id: int, payload: bytes) -> None:
    idx = PeerConnection.parse_index_payload(payload)
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    
    # Log the request message
    self.logger.recRequest(str(self.my_peer_id), str(remote_id), idx, now)
    
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
        
        # Only log if there are preferred neighbors
        if self.preferred_neighbors:  # Add this check
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
        # Don't exit for at least 30 seconds
        if time.time() - self.start_time < 30:
            return False
        
        # We must be complete first
        if not self.pm.complete():
            return False
        
        # Add ourselves to completed peers if we're complete
        if self.my_peer_id not in self.completed_peers:
            self.completed_peers.add(self.my_peer_id)
        
        with self.neighbors_lock:
            # Update completed peers based on current neighbor states
            for pid, ns in self.neighbors.items():
                if ns.bitfield.complete():
                    self.completed_peers.add(pid)
            
            # Check if we know about all expected peers being complete
            # This is conservative - only exit when we're confident everyone is done
            if len(self.completed_peers) >= len(self.all_expected_peers):
                return True
        
        return False


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
