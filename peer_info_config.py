# peer_info_config.py
from dataclasses import dataclass
from typing import Dict, List

@dataclass
class RemotePeerInfo:
    peerId: str
    peerAddress: str
    peerPort: int
    containsFile: int

class PeerInfoConfig:
    def __init__(self) -> None:
        self._peerInfoMap: Dict[str, RemotePeerInfo] = {}
        self._peerList: List[str] = []

    def loadConfigFile(self, path: str = "PeerInfo.cfg") -> None:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                tokens = line.split()
                if len(tokens) < 4:
                    continue
                pid, host, port, has = tokens[0], tokens[1], tokens[2], tokens[3]
                self._peerInfoMap[pid] = RemotePeerInfo(
                    peerId=pid, peerAddress=host, peerPort=int(port), containsFile=int(has)
                )
                self._peerList.append(pid)

    def getPeerConfig(self, peerID: str) -> RemotePeerInfo:
        return self._peerInfoMap[peerID]

    def getPeerInfoMap(self) -> Dict[str, RemotePeerInfo]:
        return self._peerInfoMap

    def getPeerList(self) -> List[str]:
        return self._peerList