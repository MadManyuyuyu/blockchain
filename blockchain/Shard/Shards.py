import random
import sys

from utils.calPart import getLuckyDog

from blockchain.Block.BCBlock import *
import numpy as np
from blockchain.Message.InforZip import TempSmShardInfor
from blockchain.Shard.BaseShards import CommonShard


class SemanticShard(CommonShard):

    def __init__(self):

        shardAddr = "smShard-" + returnUniAddr()

        super().__init__(shardAddr)
        self.cmNodesHistory = None
        self.committeeNode = None
        self.smNodesInfor = None
        self.rlyNodes = None

        self.tempInforToCmShard = None

        self.shardIndexTool = None

        self.temSmTxsPool = None

        self.subCorpus = None

        self.consensusTactic = None

        self.uploadingers = None

    def addTxsToPool(self, semanticTxs):

        if self.temSmTxsPool is None:
            self.temSmTxsPool = []

        if type(semanticTxs) == list:

            for smTx in semanticTxs:
                txIntoPoolTime = getCurrentTime()
                self.temSmTxsPool.append(smTx)
                smTx.setIntoShardPoolTime(self.shardAddr, txIntoPoolTime)

        else:

            txIntoPoolTime = getCurrentTime()
            self.temSmTxsPool.append(semanticTxs)
            semanticTxs.setIntoShardPoolTime(self.shardAddr, txIntoPoolTime)

        return len(self.temSmTxsPool)

    def addNodesToShard(self, nodes):

        if self.smNodesInfor == None:
            self.smNodesInfor = {}

        if type(nodes) == list:

            for node in nodes:
                self.smNodesInfor[node.nodeAddr] = node
        else:

            self.smNodesInfor[nodes.nodeAddr] = nodes

    def removeNodesFromShard(self, smNodeAddrs):

        if self.smNodesInfor == None:
            return False

        if type(smNodeAddrs) == list:
            removedNodeList = []

            for smNodeAddr in smNodeAddrs:
                if smNodeAddr not in self.smNodesInfor:
                    return False
                removedNode = self.smNodesInfor.pop(smNodeAddr)
                removedNodeList.append(removedNode)
            return removedNodeList
        else:

            if smNodeAddrs not in self.smNodesInfor:
                return False
            removedNode = self.smNodesInfor.pop(smNodeAddrs)
            return removedNode

    def setCmNode(self, committeeNode):

        self.committeeNode = committeeNode
        self.committeeNode.setInspectingShardAddr(self.shardAddr)
        self.__setCmNodeHistory(committeeNode.nodeAddr)

    def __setCmNodeHistory(self, cmNodeAddr):

        if self.cmNodesHistory == None:
            self.cmNodesHistory = []
        self.cmNodesHistory.append(cmNodeAddr)

    def setShardIndexTool(self, shardIndexTool):

        self.shardIndexTool = shardIndexTool
        return True

    def addCorpus(self, vectors):

        if self.subCorpus is None:
            self.subCorpus = []
            self.subCorpus.append(vectors)
            self.subCorpus = np.vstack(self.subCorpus)

        else:
            self.subCorpus = np.vstack((self.subCorpus, vectors))

    def receiveSmTx(self, smTx: SemanticTransaction, controller, cmShard=None):

        if smTx.receiver != self.shardAddr:

            if self.rlyNodes is None:
                self.__selectRlyNodes()
            rlyPosition = getLuckyDog(len(self.rlyNodes))
            rlyNode = self.rlyNodes[rlyPosition]
            rlyNode.relayTxs(smTx, controller=controller, cmShard=cmShard)
        else:

            txNodeAddr = smTx.sender
            if txNodeAddr in self.smNodesInfor:
                self.leaveTraces("increaseInShardTxs", {"txsNum": 1})

            else:

                self.leaveTraces("leaveCrossTxsHistory", {"uploadNodeAddr": smTx.sender})
                self.leaveTraces("increaseTITxsNum", {"txsNum": 1})
            self.addTxsToPool(smTx)

    def getRlyNodes(self):

        if self.rlyNodes == None:
            self.__selectRlyNodes()
        return self.rlyNodes

    def __selectRlyNodes(self):

        self.rlyNodes = []
        for nodeAddr, node in self.smNodesInfor.items():
            if nodeAddr.startswith("rly"):
                self.rlyNodes.append(node)

        if len(self.rlyNodes) == 0:
            sys.exit()

    def setConsensusTactic(self, csTactic):

        self.consensusTactic = csTactic

    def letUsRockNRoll(self):

        if self.temSmTxsPool is None or len(self.temSmTxsPool) == 0:
            return None
        if self.consensusTactic is None:
            from blockchain.Consensus.SemanticConsensus import SmShardConsensus

            self.setConsensusTactic(SmShardConsensus())

        newBlocks = self.consensusTactic.executeConsensus(
            smNodes=list(self.smNodesInfor.values()),
            blockFactory=self.blockFactory,
            tempTxsPool=self.temSmTxsPool,
            lastBlock=self.blocksList[-1] if self.blocksList is not None else None,
            cmNodeAddr=self.committeeNode.nodeAddr,
            difficulty=-1)

        for newBlock in newBlocks:
            smData = newBlock.blockBody.semanticTXs[0].semanticData

            self.addCorpus(smData)
        return self.addBlocks(newBlocks)

    def leaveTraces(self, instruction, data: dict):

        if self.tempInforToCmShard is None:
            self.tempInforToCmShard = TempSmShardInfor(self.shardAddr)

        inforInstructions = self.tempInforToCmShard.temInforInstructions
        if instruction not in inforInstructions:

            sys.exit()
        else:
            action = getattr(self.tempInforToCmShard, instruction)
            action(**data)

    def addUploader(self, node):

        if self.uploadingers is None:
            self.uploadingers = set()
        if node.nodeAddr not in self.uploadingers:
            self.uploadingers.add(node.nodeAddr)

    def __delUploader(self, node):

        if self.uploadingers is not None:
            self.uploadingers.discard(node.nodeAddr)
        if len(self.uploadingers) == 0:
            self.uploadingers = None

    def getRawTxsAndDistribute(self, rawTxsList):

        rlyNodes = self.getRlyNodes()
        if len(rlyNodes) == 0:
            sys.exit()
        for rawTx in rawTxsList:
            rlyNode = random.choice(rlyNodes)
            rlyNode.addRawTx(rawTx)

        return True

    def oldDLReceiveRaw(self, numOfDataAfterSplitting):

        numberOfRlyNode = 5

        numOfRlyNodeRlyRaw = numOfDataAfterSplitting // numberOfRlyNode
        timeOfDL = 0
        for _ in range(int(numOfRlyNodeRlyRaw)):
            timeOfDL += (28 * 2)

        timeOfQues = timeOfDL + 28

        return timeOfQues

    def newDLReceiveRaw(self, numOfDataAfterSplitting, winSize=4):

        numberOfRlyNode = 5
        timeOfDL = 0
        for _ in range(0, int(numOfDataAfterSplitting), int(winSize * numberOfRlyNode)):
            timeOfDL += (28 * 2)
        timeOfQues = timeOfDL + 28
        return timeOfQues


class CommitteeShard(CommonShard):

    def __init__(self,
                 cmInforVerifier,
                 ):
        sharAddr = "cmShard-" + returnUniAddr()
        super().__init__(sharAddr)
        self.consensusTactic = None
        self.cmInforVerifier = cmInforVerifier

        self.shardsActionsInforPool = None

        self.cmNodesInfor = None

        self.totalInShardTxs = None
        self.totalCrossShardTxs = None
        self.totalShardsNum = None

        self.shardsToBeSplited = None
        self.shardsToBeMerged = None

        self.temCrossTxsNum = None
        self.temCrossTxsNumList = None

    def addNodesToShard(self, nodes):

        if self.cmNodesInfor == None:
            self.cmNodesInfor = {}

        if type(nodes) == list:

            for node in nodes:
                self.cmNodesInfor[node.nodeAddr] = node
        else:

            self.cmNodesInfor[nodes.nodeAddr] = nodes

    def setTotalShardNum(self, shardsNum):

        if self.totalShardsNum is None:
            self.totalShardsNum = []
        self.totalShardsNum.append(shardsNum)

    def setTotalInShardTxs(self, inshardTxs):

        if self.totalInShardTxs is None:
            self.totalInShardTxs = []
        self.totalInShardTxs.append(inshardTxs)

    def setTotalCrossShardTxs(self, crossShardTxs):

        if self.totalCrossShardTxs is None:
            self.totalCrossShardTxs = []
        self.totalCrossShardTxs.append(crossShardTxs)

    def setShardsToBeSplited(self, shardsToBeSplited):

        if self.shardsToBeSplited is None:
            self.shardsToBeSplited = []
        if shardsToBeSplited is None:
            shardsToBeSplited = []
        self.shardsToBeSplited.append(shardsToBeSplited)

    def setShardsToBeMerged(self, shardsToBeMerged):

        if self.shardsToBeMerged is None:
            self.shardsToBeMerged = []
        if shardsToBeMerged is None:
            shardsToBeMerged = []
        self.shardsToBeMerged.append(shardsToBeMerged)

    def removeNodesFromShard(self, cmNodeAddrs):

        if self.cmNodesInfor == None:
            return False
        if type(cmNodeAddrs) == list:
            removedNodesList = []

            for cmNodeAddr in cmNodeAddrs:
                if cmNodeAddr not in self.cmNodesInfor:
                    return False
                removedNode = self.cmNodesInfor.pop(cmNodeAddr)
                removedNodesList.append(removedNode)
            return removedNodesList

        else:

            if cmNodeAddrs not in self.cmNodesInfor:
                return False
            removedNode = self.cmNodesInfor.pop(cmNodeAddrs)
            return removedNode

    def receiveShardsInfor(self, temShardInfor: TempSmShardInfor):

        if self.shardsActionsInforPool is None:
            self.shardsActionsInforPool = []
        self.shardsActionsInforPool.append(temShardInfor)
        return len(self.shardsActionsInforPool)

    def setConsensusTactic(self, csTactic):

        self.consensusTactic = csTactic

    def letUsRockNRoll(self):

        if self.consensusTactic is None:
            sys.exit()
        else:

            smShardsShardingStrategy = self.consensusTactic.executeConsensus(
                cmVerifier=self.cmInforVerifier,
                shardsActionsInforPool=self.shardsActionsInforPool
            )
            self.setTotalCrossShardTxs(smShardsShardingStrategy.totalCrossShardTxs)
            self.setTotalInShardTxs(smShardsShardingStrategy.totalInShardTxs)

            self.setShardsToBeSplited(smShardsShardingStrategy.shardsToBeSplited)
            self.setShardsToBeMerged(smShardsShardingStrategy.shardsToBeMerged)

            cmBlock = self.__getCMBlock(smShardsShardingStrategy)
            self.addBlocks(cmBlock)

    def __getCMBlock(self, smShardsShardingStrategy):

        headerData = {}
        if self.blocksList is None:

            headerData["prevhash"] = "-1"
        else:
            headerData["prevhash"] = self.blocksList[-1].blockHeader.blockAddr

        headerData["minerAddr"] = "-1"
        headerData["difficulty"] = -1
        headerData["committeeNodeAddr"] = "-1"
        cmHeader = SemanticBlockHeader(**headerData)

        cmBody = CommitteeBlockBody(nodesActionsList=self.shardsActionsInforPool)

        cmBlock = CommitteeBlock(blockHeader=cmHeader, blockBody=cmBody,
                                 shardingStrategy=smShardsShardingStrategy)

        return cmBlock
