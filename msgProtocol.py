# This is the class definition(s) for each type of message possible in the torrent client.

import struct
import bitarray
import string

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
    def encode(self):
        # gets message information ready for transmission
        # each type of message will probably need to overwrite this, so leaving it empty
        pass

    def decode(self, input):
        # gets bytes ready to be stored/acted on
        pass

# message length: 1
class interested(genMessage):
    def encode(self) -> bytes:
        # prepare interested message, no payload
        return struct.pack('>i', 1, 2)  #code:2
    
    def decode(self, input: bytearray):
        msgLength = struct.unpack('>i', input[:4])
        msgCode = struct.unpack('>i', input[4:5])
        return msgLength, msgCode

class notInterested(genMessage):
    def encode(self) -> bytes:
        # prepare not interested message, no payload
        return struct.pack('>i', 1, 3)  #code:3
    def decode(self, input: bytearray):
        msgLength = struct.unpack('>i', input[:4])
        msgCode = struct.unpack('>i', input[4:5])
        return msgLength, msgCode
    
class choke(genMessage):
    # prepare choke, no payload
    def encode(self):
        return struct.pack('>i', 1, 0)  #code:0
    def decode(self, input: bytearray):
        msgLength = struct.unpack('>i', input[:4])
        msgCode = struct.unpack('>i', input[4:5])
        return msgLength, msgCode

class unchoke(genMessage):
    def encode(self):
        return struct.pack('>i', 1, 1)  #code:1
    def decode(self, input: bytearray):
        msgLength = struct.unpack('>i', input[:4])
        msgCode = struct.unpack('>i', input[4:5])
        return msgLength, msgCode

# more complicated
class handshake(genMessage):
    # idea: storage containers of empty byte arrays, fill in with big endian-ified info using pack_into
    header = bytearray(18)
    zeroBits = bytearray(10)
    peerID = bytearray(4)
    fullHandshake = bytearray(32)

    # constructor
    def __init__(self, peerID):
        # ref: https://stackoverflow.com/questions/56799021/how-to-efficiently-store-an-int-into-bytes
        # pack bytes into storage arrays
        struct.pack_into('>s', self.header, 0, "P2PFILESHARINGPROJ")
        struct.pack_into('>', self.peerID, 0, peerID.to_bytes(4)) # heeds to be 4 bytes specifically
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
            # check if each piece is valid: project-defined header, padding, and the last four are digits (peerID)
            if (oneThird == self.header and twoThird == self.zeroBits and threeThird.isdigit()):
                return True # valid, can start connection
        else:
            return False # invalid, cancel connection
        
class bitfield(genMessage):
    # actual data stored externally - this is just built for sending/receiving messages
    def encode(self, bitfield):
        # format: 01100111, 10001000, etc.
        # bitfield: stored as string
        msgLength = len(bitfield) + 1
        return struct.pack('>iis', msgLength.to_bytes(4), 5, bitfield)  #code:5
    
    def decode(self, input: bytearray):
        msgLength = struct.unpack('>i', input[:4])
        msgCode = struct.unpack('>i', input[4:5])
        msgPayload = struct.unpack('>s', input[5:])
        return msgLength, msgCode, msgPayload
    
class have(genMessage):
    def encode(self, index):
        msgLength = len(index) + 1
        return struct.pack('>iii', msgLength, 4, index)     #code:4
    def decode(self, input):
        msgLength = struct.unpack('>i', input[:4])
        msgCode = struct.unpack('>i', input[4:5])
        msgPayload = struct.unpack('>i', input[5:])
        return msgLength, msgCode, msgPayload
    
class request(genMessage):
    def encode(self, index):
        msgLength = len(index) + 1
        return struct.pack('>iii', msgLength, 6, index)     #code:6
    def decode(self, input):
        msgLength = struct.unpack('>i', input[:4])
        msgCode = struct.unpack('>i', input[4:5])
        msgPayload = struct.unpack('>i', input[5:])
        return msgLength, msgCode, msgPayload

class piece(genMessage):
    def encode(self, index, content):
        msgLength = len(index) + len(content) + 1
        return struct.pack('>iiib', msgLength, 7, index, content)
    def decode(self, input):
        msgLength = struct.unpack('>i', input[:4])
        msgCode = struct.unpack('>i', input[4:5])
        msgIndex = struct.unpack('>i', input[5:10])
        msgPayload = struct.unpack('>b', input[10:])
        return msgLength, msgCode, msgIndex, msgPayload