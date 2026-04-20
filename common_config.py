# common_config.py
from dataclasses import dataclass

@dataclass
class CommonConfig:
    NumberOfPreferredNeighbors: int = 0
    UnchokingInterval: int = 0
    OptimisticUnchokingInterval: int = 0
    FileName: str = ""
    FileSize: int = 0
    PieceSize: int = 0

    def loadCommonFile(self, path: str = "Common.cfg") -> None:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split()
                if len(parts) < 2:
                    continue
                key, val = parts[0], parts[1]
                if key == "NumberOfPreferredNeighbors":
                    self.NumberOfPreferredNeighbors = int(val)
                elif key == "UnchokingInterval":
                    self.UnchokingInterval = int(val)
                elif key == "OptimisticUnchokingInterval":
                    self.OptimisticUnchokingInterval = int(val)
                elif key == "FileName":
                    self.FileName = val
                elif key == "FileSize":
                    self.FileSize = int(val)
                elif key == "PieceSize":
                    self.PieceSize = int(val)