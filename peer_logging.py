# class for logging events
from pathlib import Path

class peerLogger:
    def __del__(self):
        print("Closing logger...")

    def estConnection(self, ID1, ID2, time):
        # check if read message properly
        if (ID1 and ID2):   # non empty
            peerFile1 = "peer_" + ID1 + "/log_peer_" + ID1 + ".log"
            peerFile2 = "peer_" + ID2 + "/log_peer_" + ID2 + ".log"
            # NOTE: ENSURE IN IMPLEMENTATION - ID1 is the connector, ID2 is the connectee
            with open(peerFile1, 'a') as file:
                file.write(f"{time}: Peer {ID1} makes a connection to Peer {ID2}.\n")
                print(f"{time}: Peer {ID1} makes a connection to Peer {ID2}.\n")
            with open(peerFile2, 'a') as file:
                file.write(f"{time}: Peer {ID2} is connected from Peer {ID1}.\n")
                print(f"{time}: Peer {ID2} is connected from Peer {ID1}.\n")
        else:
            print("ERROR: estConnection")

    def changePrefNeighbor(self, ID, idList: list, time):
        if (ID):
            fileName = "peer_" + ID + "/log_peer_" + ID + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID} has the preferred neighbors {idList}.\n")
        else:
            print("ERROR: changePrefNeighbor")

    def changeOUN(self, ID1, ID2, time):    # OUN = optimistically unchoked neighbor
        if (ID1 and ID2):
            # ID 1 = changer ; ID 2 = neighbor
            fileName = "peer_" + ID1 + "/log_peer_" + ID1 + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID1} has the optimistically unchoked neighbor {ID2}.\n")
        else:
            print("ERROR: changeOUN")

    def unchoking(self, ID1, ID2, time):
        # ID 1 = unchoker ; ID 2 = logger
        if (ID1 and ID2):
            fileName = "peer_" + ID2 + "/log_peer_" + ID2 + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID2} is unchoked by {ID1}.\n")
        else:
            print("ERROR: unchoking")

    def choking(self, ID1, ID2, time):
        # ID 1 = choker ; ID 2 = logger
        if (ID1 and ID2):
            fileName = "peer_" + ID2 + "/log_peer_" + ID2 + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID2} is choked by {ID1}.\n")
        else:
            print("ERROR: choking")

    def recHave(self, ID1, ID2, index, time, bitfield):
        if (ID1 and ID2):
            # ID 1 = receiver ; ID 2 = sender
            fileName = "peer_" + ID1 + "/log_peer_" + ID1 + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID1} received the 'have' message from {ID2} for the piece {index}. Updated bitfield: {bitfield}\n")
        else:
            print("ERROR: recHave")
    
    def recInterest(self, ID1, ID2, time):
        if (ID1 and ID2):
            # ID 1 = receiver ; ID 2 = sender
            fileName = "peer_" + ID1 + "/log_peer_" + ID1 + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID1} received the 'interested' message from {ID2}.\n")
        else:
            print("ERROR: recInterest")

    def recNotInterest(self, ID1, ID2, time):
        if (ID1 and ID2):
            # ID 1 = receiver ; ID 2 = sender
            fileName = "peer_" + ID1 + "/log_peer_" + ID1 + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID1} receoved the 'not interested' message from {ID2}.\n")
        else:
            print("ERROR: recNotInterest")

    def pieceDL(self, ID1, ID2, index, number, time):
        if (ID1 and ID2):
            # ID1 = downloader ; ID2 = sender
            fileName = "peer_" + ID1 + "/log_peer_" + ID1 + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID1} has downloaded the piece {index} from {ID2}. Now the number of pieces it has is {number} (bitfield updated).\n")
        else:
            print("ERROR: pieceDL")

    def completeDL(self, ID, time):
        if (ID):
            fileName = "peer_" + ID + "/log_peer_" + ID + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID} has downloaded the complete file.")
        else:
            print("ERROR: completeDL")

    def recRequest(self, ID1, ID2, index, time):
        """Log when a REQUEST message is received"""
        if (ID1 and ID2):
            # ID1 = receiver (us) ; ID2 = sender (requester)
            fileName = "peer_" + ID1 + "/log_peer_" + ID1 + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID1} received the 'request' message from {ID2} for piece {index}.\n")
            print(f"{time}: Peer {ID1} received the 'request' message from {ID2} for piece {index}.")
        else:
            print("ERROR: recRequest")
