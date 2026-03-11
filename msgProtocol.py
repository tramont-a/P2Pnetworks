# This is the class definition(s) for each type of message possible in the torrent client.

from bitstring import BitArray

# (N/A)   Handshake - 32 bytes
#     [ Header: 'P2PFILESHARINGPROJ' (18 bytes) | Zero bits (10 bytes) | peer ID, int (4 bytes) ]

# Generic Message Format
#     [ Message length (4 bytes) | message type (1 byte) | message payload (variable) ]

# 0   Choke

# 1   Unchoke

# 2   Interested

# 3   Not Interested

# 4   Have
#     - Payload: 4 byte piece index field

# 5   Bitfield
#     - First message after handshake completes; established connection. 
#     - Payload: bitfield. Uses big endian (high to low bit)
#         - First byte: Indicies 0 - 7
#         - Second byte: Indicies 8 - 15
#         - Spare bits = 0
#     - If peer doesn't have anything, ignore bitfield message.

# 6   Request
#     - Payload: 4 byte piece index field 
#     - No smaller subpieces (unlike BitTorrent)

# 7   Piece
#     - Payload: 4 byte piece index field and content of piece

class handshake:
    header = bytearray("P2PFILESHARINGPROJ")
    zeroBits = bytearray(10)
    peerID = bytearray(4)

    # constructor
    def __init__(self, peerID):
        # ref: https://stackoverflow.com/questions/56799021/how-to-efficiently-store-an-int-into-bytes
        self.peerID = peerID.to_bytes(4, 'big')

class genMessage:
    # Message flags
    choke = 0
    unchoke = 1
    interested = 2
    notInterested = 3
    have = 4
    bitField = 5
    request = 6
    piece = 7