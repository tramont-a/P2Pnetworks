# peer_logger.py
import logging
from datetime import datetime
from typing import List

class PeerLogger:
    def __init__(self, peer_id: str):
        self.peer_id = peer_id
        self.log_file_name = f"log_peer_{self.peer_id}.log"
        self._logger = logging.getLogger(f"PeerLogs-{self.peer_id}")
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False
        self._handler = logging.FileHandler(self.log_file_name, mode="w", encoding="utf-8")
        # Match Java format: "[dd-MMM-yyyy hh:mm:ss AM/PM]: message"
        fmt = logging.Formatter("%(message)s")
        self._handler.setFormatter(fmt)
        self._logger.addHandler(self._handler)

    def _ts(self) -> str:
        return datetime.now().strftime("%d-%b-%Y %I:%M:%S %p")

    def _log(self, msg: str) -> None:
        self._logger.info(f"[{self._ts()}]: {msg}")

    # TCP connection
    def genTCPConnLogSender(self, peer: str) -> None:
        self._log(f"Peer [{self.peer_id}] makes a connection to Peer [{peer}].")

    def genTCPConnLogReceiver(self, peer: str) -> None:
        self._log(f"Peer [{self.peer_id}] is connected from Peer [{peer}].")

    # Change of preferred neighbors
    def changePreferredNeigbors(self, neighbors: List[str]) -> None:
        neigh_list = ",".join(neighbors) if neighbors else ""
        self._log(f"Peer [{self.peer_id}] has the preferred neighbors [{neigh_list}].")

    # Change of optimistically unchoked neighbor
    def changeOptimisticallyUnchokedNeighbor(self, peer: str) -> None:
        self._log(f"Peer [{self.peer_id}] has the optimistically unchoked neighbor [{peer}].")

    # Unchoking / choking
    def unchokedNeighbor(self, peer: str) -> None:
        self._log(f"Peer [{self.peer_id}] is unchoked by [{peer}].")

    def chokingNeighbor(self, peer: str) -> None:
        self._log(f"Peer [{self.peer_id}] is choked by [{peer}].")

    # Receiving messages
    def receiveHave(self, peer: str, index: int) -> None:
        self._log(
            f"Peer [{self.peer_id}] received the 'have' message from [{peer}] for the piece [{index}]."
        )

    def receiveInterested(self, peer: str) -> None:
        self._log(f"Peer [{self.peer_id}] received the 'interested' message from [{peer}].")

    def receiveNotInterested(self, peer: str) -> None:
        self._log(f"Peer [{self.peer_id}] received the 'not interested' message from [{peer}].")

    # Piece download and completion
    def downloadPiece(self, peer: str, ind: int, pieces: int) -> None:
        self._log(
            f"Peer [{self.peer_id}] has downloaded the piece [{ind}] from [{peer}]. "
            f"Now the number of pieces it has is [{pieces}]."
        )

    def downloadComplete(self) -> None:
        self._log(f"Peer [{self.peer_id}] has downloaded the complete file.")

    def closeLogger(self) -> None:
        try:
            if self._handler:
                self._handler.close()
                self._logger.removeHandler(self._handler)
        except Exception:
            pass