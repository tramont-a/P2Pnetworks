# This is the class definition(s) for each type of message possible in the torrent client.

import struct

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

# bytearray documentation - https://docs.python.org/3/library/stdtypes.html#bytearray-objects

# generic message template
class genMessage:
    # Message flags
    choke = 0
    unchoke = 1
    interested = 2
    notInterested = 3
    have = 4
    bitfield = 5
    request = 6
    piece = 7

    def encode(self) -> bytes:
        # gets message information ready for transmission
        # each type of message will probably need to overwrite this, so leaving it empty
        pass

    def decode(self, input):
        # gets bytes ready to be stored/acted on
        pass

class handshake(genMessage):
    # idea: storage containers of empty byte arrays, fill in with big endian-ified info using pack_into
    header = bytearray(18)
    zeroBits = bytearray(10)
    peerID = bytearray(4)
    fullHandshake = bytearray(32)

    # constructor
    def __init__(self, peerID):
        # ref: https://stackoverflow.com/questions/56799021/how-to-efficiently-store-an-int-into-bytes
        self.header = struct.pack_into('>s', "P2PFILESHARINGPROJ")
        self.peerID = struct.pack_into('>', peerID.to_bytes(4)) # take read integers, convert to 4 bytes in big endian, store
        self.fullHandshake = self.header + self.zeroBits + self.peerID
    
    def encode(self) -> bytes:
        return self.fullHandshake
    
    def decode(self, input: bytearray):
        # check if valid handshake - correct size
        if (len(input) == 32):
            # slice input
            oneThird = input[:18]
            twoThird = input[18:29]
            threeThird = input[29:]
            # check if each piece is valid
            if (oneThird == self.header and twoThird == self.zeroBits and threeThird.isdigit()):
                return True # valid, can start connection
        else:
            return False # invalid, cancel connection
