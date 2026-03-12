class TempSmShardInfor:

    def __init__(self, shardAddr=None):
        self.shardAddr = shardAddr
        self.inShardTxsNum = 0
        self.sendOutTxsNum = 0
        self.takeInTxsNum = 0

        self.smNodesInitTxsHistory = None
        self.crossShardTxsHistory = None

        self.relayNodesTxsHistory = None

        self.temInforInstructions = None
        self.returnInstructions()

    def increaseInShardTxs(self, txsNum=1):

        self.inShardTxsNum += txsNum
        return self.inShardTxsNum

    def increaseSOTxsNum(self, txsNum=1):

        self.sendOutTxsNum += txsNum
        return self.sendOutTxsNum

    def increaseTITxsNum(self, txsNum=1):

        self.takeInTxsNum += txsNum
        return self.takeInTxsNum

    def leaveSmNodeHistory(self, uploadNodeAddr, targetAddr):

        if self.smNodesInitTxsHistory is None:
            self.smNodesInitTxsHistory = []
        self.smNodesInitTxsHistory.append((uploadNodeAddr, targetAddr))
        return len(self.smNodesInitTxsHistory)

    def leaveCrossTxsHistory(self, uploadNodeAddr):

        if self.crossShardTxsHistory is None:
            self.crossShardTxsHistory = []
        self.crossShardTxsHistory.append(uploadNodeAddr)

    def leaveRlyTxsHistory(self, rlyNodeAddr, targetShardAddr):

        if self.relayNodesTxsHistory is None:
            self.relayNodesTxsHistory = []
        self.relayNodesTxsHistory.append((rlyNodeAddr, targetShardAddr))

    def returnInstructions(self):

        ins = {name for name, value in TempSmShardInfor.__dict__.items() if callable(value)}

        ins.remove("__init__")

        ins.remove("returnInstructions")
        self.temInforInstructions = ins

    def __str__(self):
        pass
