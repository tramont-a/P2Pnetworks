# class for logging events

class logger:
    def estConnection(self, ID1, ID2, time):
        # check if read message properly
        if (ID1 and ID2):   # non empty
            peerFile1 = "log_peer_" + "log_peer_" + ID1 + ".log"
            peerFile2 = "log_peer_" + ID2 + ".log"
            # NOTE: ENSURE IN IMPLEMENTATION - ID1 is the connector, ID2 is the connectee
            with open(peerFile1, 'w') as file:
                file.write(f"{time}: Peer {ID1} makes a connection to Peer {ID2}.\n")
            with open(peerFile2, 'w') as file:
                file.write(f"{time}: Peer {ID2} is connected from Peer {ID1}.\n")
        else:
            print("ERROR: estConnection")

    def changePrefNeighbor(self, ID, idList: list, time):
        if (ID):
            fileName = "log_peer_" + ID + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID} has the preferred neighbors {idList}.\n")
        else:
            print("ERROR: changePrefNeighbor")

    def changeOUN(self, ID1, ID2, time):    # OUN = optimistically unchoked neighbor
        if (ID1 and ID2):
            # ID 1 = changer ; ID 2 = neighbor
            fileName = "log_peer_" + ID1 + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID1} has the optimistically unchoked neighbor {ID2}.\n")
        else:
            print("ERROR: changeOUN")

    def unchoking(self, ID1, ID2, time):
        # ID 1 = unchoker ; ID 2 = logger
        if (ID1 and ID2):
            fileName = ID2 + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID2} is unchoked by {ID1}.\n")
        else:
            print("ERROR: unchoking")

    def choking(self, ID1, ID2, time):
        # ID 1 = choker ; ID 2 = logger
        if (ID1 and ID2):
            fileName = ID2 + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID2} is choked by {ID1}.\n")
        else:
            print("ERROR: choking")

    def recHave(self, ID1, ID2, index, time):
        if (ID1 and ID2):
            # ID 1 = receiver ; ID 2 = sender
            fileName = "log_peer_" + ID1 + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID1} received the 'have' message from {ID2} for the piece {index}.\n")
        else:
            print("ERROR: recHave")
    
    def recInterest(self, ID1, ID2, time):
        if (ID1 and ID2):
            # ID 1 = receiver ; ID 2 = sender
            fileName = "log_peer_" + ID1 + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID1} received the 'interested' message from {ID2}.\n")
        else:
            print("ERROR: recInterest")

    def recNotInterest(self, ID1, ID2, time):
        if (ID1 and ID2):
            # ID 1 = receiver ; ID 2 = sender
            fileName = "log_peer_" + ID1 + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID1} receoved the 'not interested' message from {ID2}.\n")
        else:
            print("ERROR: recNotInterest")

    def pieceDL(self, ID1, ID2, index, number, time):
        if (ID1 and ID2):
            # ID1 = downloader ; ID2 = sender
            fileName = "log_peer_" + ID1 + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID1} has downloaded the piece {index} from {ID2}. Now the number of pieces it has is {number}.\n")
        else:
            print("ERROR: pieceDL")

    def completeDL(self, ID, time):
        if (ID):
            fileName = "log_peer_" + ID + ".log"
            with open(fileName, 'a') as file:
                file.write(f"{time}: Peer {ID} has downloaded the complete file.")
        else:
            print("ERROR: completeDL")