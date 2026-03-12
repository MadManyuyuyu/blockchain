import random
import sys
from abc import ABC

from blockchain.Node.FatherNode import BaseNode
from blockchain.BlockFactory import BlockFactory
from blockchain.Message.BCMessage import *
from blockchain.Shard.Shards import CommitteeShard
from utils.calPart import getLuckyDog
from utils.getUniAddr import returnUniAddr
from blockchain.Transcation.SemanticTX import SemanticTransaction
from blockchain.downloadPart.slidingWind import getWindows
from utils.myQueue import MyQ


class CommitteeNode(BaseNode, ABC):

    def __init__(self,
                 nodeLocation

                 ):
        super().__init__(nodeLocation)

        self.csMethodsHistory = []
        self.inspectingShardAddr = None
        self.inspectedShardsHistory = []
        self.smNodesActionsInfor = None
        self.__setNodeAddr()

    def addNodeActionInfor(self, nodeA_Addr: str, targetShardAddr: str, nodeB_Addr: str, actionCode: str):

        if self.smNodesActionsInfor == None:
            self.smNodesActionsInfor = {}
        if self.smNodesActionsInfor[nodeA_Addr] not in self.smNodesActionsInfor:
            self.smNodesActionsInfor[nodeA_Addr] = {}
        if self.smNodesActionsInfor[nodeA_Addr][targetShardAddr] not in self.smNodesActionsInfor[nodeA_Addr]:
            self.smNodesActionsInfor[nodeA_Addr][targetShardAddr] = set()
        totalActionTimes = len(self.smNodesActionsInfor[nodeA_Addr][targetShardAddr])
        totalActionTimes += 1
        addrAndCode = nodeB_Addr + "-" + actionCode + "-" + str(totalActionTimes)
        self.smNodesActionsInfor[nodeA_Addr][targetShardAddr].add(addrAndCode)
        return len(self.smNodesActionsInfor[nodeA_Addr][targetShardAddr])

    def handleNodesActionsToCmShard(self, cmShard: CommitteeShard):

        numOfHandled = cmShard.receiveShardsInfor(self.smNodesActionsInfor)
        return numOfHandled

    def __setNodeAddr(self):
        self.nodeAddr = "cmNode-" + returnUniAddr()

    def setInspectingShardAddr(self, shardAddr):

        self.inspectingShardAddr = shardAddr
        self.__addInspectedShardToHistory(self.inspectingShardAddr)
        return self.inspectingShardAddr

    def __addInspectedShardToHistory(self, shardAddr):

        pass

    def __addCsMethodsToHistory(self, csMethod):

        pass


class SemanticNode(BaseNode):

    def __init__(self,
                 nodeLocation,
                 hashRate=0
                 ):
        super().__init__(nodeLocation)
        self.hashRate = hashRate

        self.uploadedTxsList = []
        self.downloadedTxsList = []

        self.belongedShardsList = None
        self.nextEpochInclinedShardAddr = None
        self.blocksPackedList = []

        self.rawDataWindowSizeDict = None
        self.rawDataDict = None

        self.groupNum = None
        self.__setNodeAddr()

    def __informAction2CmNode(self, cmNodeAddr, actionInfor):

        pass

    def __setNodeAddr(self):
        self.nodeAddr = "smNode-" + returnUniAddr()

    def queryTxs(self, queryMessage: QueryMessage):

        pass

    def uploadTxs(self, semanticData, controller, cmShard=None, uploadMessage=None):

        nearsetShard = controller.getNearestShards(semanticData=semanticData)
        txInfor = {}

        txInfor["sender"] = self.nodeAddr
        txInfor["receiver"] = nearsetShard.shardAddr
        txInfor["signature"] = "Lana Lee"
        txInfor["semanticData"] = semanticData
        smTx = SemanticTransaction(**txInfor)
        belongingShard = controller.shardsInfor[self.belongingChainAddr]

        belongingShard.leaveTraces("leaveSmNodeHistory", {"uploadNodeAddr": self.nodeAddr,
                                                          "targetAddr": smTx.receiver})

        belongingShard.receiveSmTx(smTx, controller, cmShard=cmShard)

    def __addUploadedTxs(self, uploadedTxs: list):

        pass

    def downloadTxs(self, downloadMessage: DownloadMessage):

        pass

    def __addDownloadedTxs(self, downoadedTxs: list):

        pass

    def __addQueriesToHistory(self, queries: list):

    def setNodeBelongingChainAddr(self, shardAddr):

        self.belongingChainAddr = shardAddr
        self.__addBelongedShard(self.belongingChainAddr)

    def __addBelongedShard(self, shardAddr):

        if self.belongedShardsList == None:
            self.belongedShardsList = []
        self.belongedShardsList.append(shardAddr)

    def setNtEpInclinedShardAddr(self, ntEpShardAddr=None):

        self.nextEpochInclinedShardAddr = ntEpShardAddr
        return self.nextEpochInclinedShardAddr

    def packAndProduceBlock(self, txs, blockFactory: BlockFactory, lastBlockAddr, cmNodeAddr, difficulty=-1):

        headData = {}
        if lastBlockAddr == None:
            headData["prevhash"] = "-1"
        else:
            headData["prevhash"] = lastBlockAddr
        headData["minerAddr"] = self.nodeAddr
        headData["difficulty"] = difficulty
        headData["committeeNodeAddr"] = cmNodeAddr

        smBlock = blockFactory.getBlockWithTxs(headData, txs)
        return smBlock

    def __addPackedBlock(self, blockAddr):

        pass

    def upLoadRawData(self, semanticData, windSize, receiver, controller):

        isExistRaw = self.__isExistRaw(semanticData=semanticData)
        if isExistRaw is False:
            pass
        uniqueId, rawDataList = self.__retrievalRaw(semanticData=semanticData, sizeOfRaw=20)

        self.__addRawWindowSizeDict(uniqueId=uniqueId, windSize=windSize)
        self.__addTemRawDataDict(uniqueId=uniqueId, rawList=rawDataList)

        gen = getWindows(dataList=rawDataList, windowSize=windSize)
        for windowData, wSize in gen:
            txsList = self.__packRawToTx(rawDataList=windowData, receiver=receiver)
            self.__uploadRawTxsToShard(txsList=txsList, controller=controller)
            gen.send(windSize)

    def __isExistRaw(self, semanticData):

        pass

    def __retrievalRaw(self, semanticData, sizeOfRaw):

        pass

    def __addRawWindowSizeDict(self, uniqueId, windSize):

        if self.rawDataWindowSizeDict is None:
            self.rawDataWindowSizeDict = {}
        self.rawDataWindowSizeDict[uniqueId] = windSize

    def __delRawWindowSizeDict(self, uniqueId):

        if self.rawDataWindowSizeDict is not None:
            self.rawDataWindowSizeDict.pop(uniqueId)
            if len(self.rawDataWindowSizeDict) == 0:
                self.rawDataWindowSizeDict = None

    def __addTemRawDataDict(self, uniqueId, rawList):

        if self.rawDataDict is None:
            self.rawDataDict = {}
        self.rawDataDict[uniqueId] = rawList

    def __delTemRawDataDict(self, uniqueId):

        if self.rawDataDict is not None:
            self.rawDataDict.pop(uniqueId)
            if len(self.rawDataDict) == 0:
                self.rawDataDict = None

    def __packRawToTx(self, rawDataList: list, receiver):

        txInfor = {}

        txInfor["sender"] = self.nodeAddr
        txInfor["receiver"] = receiver.nodeAddr
        txInfor["signature"] = "THIS IS RAW"

        txsList = []
        for rawData in rawDataList:
            txInfor["semanticData"] = rawData
            smTx = SemanticTransaction(**txInfor)
            txsList.append(smTx)
        return txsList

    def __uploadRawTxsToShard(self, txsList, controller):

        belongingShard = controller.shardsInfor[self.belongingChainAddr]
        isDone = belongingShard.getRawTxsAndDistribute(rawTxsList=txsList)

        if isDone is True:
            return True

    def receRawTx(self, rawTx):

        pass


class RelayNode(BaseNode, ABC):

    def __init__(self,

                 nodeLocation,
                 ):
        super().__init__(nodeLocation)
        self.relayedTxsList = []

        self.belongedShardsList = None

        self.rawQ = None
        self.receRawQ = None
        self.rlyQ = None

        self.controller = None

        self.__setNodeAddr()

    def __setNodeAddr(self):
        self.nodeAddr = "rlyNode-" + returnUniAddr()

    def setNodeBelongingChainAddr(self, shardAddr):

        self.belongingChainAddr = shardAddr
        self.__addBelongedShard(self.belongingChainAddr)

    def queryTxs(self, queryMessage: QueryMessage):

        pass

    def __addQueriesToHistory(self, queries: list):
        pass

    def __addRelayTxsToHistory(self, relayData):

        pass

    def relayTxs(self, smTx: SemanticTransaction, controller, cmShard=None, relayMessage=None):

        cmShard.temCrossTxsNum += 1

        rlyNodeBelongingShard = controller.shardsInfor[self.belongingChainAddr]
        rlyNodeBelongingShard.leaveTraces("increaseSOTxsNum", {"txsNum": 1})

        rlyNodeBelongingShard.leaveTraces("leaveRlyTxsHistory",
                                          {"rlyNodeAddr": self.nodeAddr, "targetShardAddr": smTx.receiver})

        receiveShard = controller.shardsInfor[smTx.receiver]
        receiveRlyNodes = receiveShard.getRlyNodes()
        receivePosition = getLuckyDog(len(receiveRlyNodes))
        receiveRlyNode = receiveRlyNodes[receivePosition]
        receiveRlyNode.takeTxAndHandleToShard(smTx, controller, cmShard=cmShard)

    def takeTxAndHandleToShard(self, smTx, controller, cmShard=None):

        if smTx.receiver == self.belongingChainAddr:

            belongingShard = controller.shardsInfor[self.belongingChainAddr]

            belongingShard.leaveTraces("leaveRlyTxsHistory", {"rlyNodeAddr": self.nodeAddr,
                                                              "targetShardAddr": smTx.receiver})

            belongingShard.receiveSmTx(smTx, controller, cmShard=cmShard)
        else:
            pass

    def setNodeBelongingChainAddr(self, shardAddr: str):

        self.belongingChainAddr = shardAddr
        self.__addBelongedShard(self.belongingChainAddr)

    def __addBelongedShard(self, shardAddr):

        pass

    def addRawTx(self, rawTx):

        if self.rawQ is None:
            self.rawQ = MyQ()
        self.rawQ.addTxToQ(rawTx)

        self.__processRawTx()

    def __removeRawTx(self):

        self.rawQ.removeTxFromQ()

    def __processRawTx(self):

        if self.rawQ is None or self.rawQ.size == 0:
            return
        sizeQ = self.rawQ.size
        for _ in range(sizeQ):
            rawTx = self.rawQ.getFrontTx()
            self.rawQ.removeTxFromQ()

            self.__rlyRawTx(rawTx=rawTx)

    def __rlyRawTx(self, rawTx):

        receiver = rawTx.receiver
        receShardAddr = receiver.belongingChainAddr
        if self.controller is None:
            sys.exit()
        if receShardAddr not in self.controller.shardsInfor:
            sys.exit()
        receShard = self.controller.shardsInfor[receShardAddr]
        receRlyNodes = receShard.getRlyNodes()
        if len(receRlyNodes) == 0:
            sys.exit()
        receRlyNode = random.choice(receRlyNodes)
        receRlyNode.getRawTxAndSend(rawTx)

    def __processRlyTx(self):

        pass

    def getRawTxAndSend(self, rawTx):

        if self.receRawQ is None:
            self.receRawQ = MyQ()
        self.receRawQ.addTxToQ(rawTx)
        self.__processReceRawTx()

    def __processReceRawTx(self):

        if self.receRawQ is None or self.receRawQ.size == 0:
            return

        sizeReceRawQ = self.receRawQ.size
        for _ in range(sizeReceRawQ):
            rawTx = self.receRawQ.getFrontTx()
            self.receRawQ.removeTxFromQ()
            receiver = rawTx[rawTx]

            receShardAddr = receiver.belongingChainAddr

            if receShardAddr != self.belongingChainAddr:
                sys.exit()

            receiver.receRawTx(rawTx)
