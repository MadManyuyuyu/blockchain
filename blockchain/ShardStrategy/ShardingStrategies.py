import sys
from abc import abstractmethod

import  random
import numpy as np

from blockchain.ShardFactory import ShardFactory
from controller.BCController import BCController


class BaseStrategy:

    def __init__(self):
        pass

    @abstractmethod
    def allocateSemanticNodes(self):
        pass


class CommitteeStrategy(BaseStrategy):

    def __int__(self):
        super().__init__()

    def allocateSemanticNodes(self, numOfNodes, numOfCmNode):
        pass


class SemanticStrategy(BaseStrategy):

    def __init__(self, rateToHotShard):
        super().__init__()
        self.rateToHotShard = rateToHotShard

        self.shardsToBeSplited = None
        self.shardsToBeMerged = None
        self.nodesInclinedShards = None
        self.isAfterMerged = False
        self.shardAddrAndCrossShardsTxs = None
        self.totalCrossShardTxs = 0
        self.shardAddrAndInShardsTxs = None
        self.totalInShardTxs = 0

    def initialDistributeNodesToShards(self, nodes: list, shards: list):

        lenShards = len(shards)
        nodeListIndexs = list(range(0, len(nodes)))
        random.shuffle(nodeListIndexs)
        for i, num in enumerate(nodeListIndexs):
            shards[i % lenShards].addNodesToShard(nodes[num])
            nodes[num].setNodeBelongingChainAddr(shards[i % lenShards].shardAddr)

        return True

    def allocateSemanticNodes(self, ):

        pass

    def allocateRelayNodes(self):

        pass

    def splitShards(self, shardFactory: ShardFactory, controller: BCController, numOfSplitting=2):

        if self.shardsToBeSplited is None or len(self.shardsToBeSplited) == 0:
            return None
        if self.isAfterMerged is False:

            sys.exit()



        else:
            for shardAddr in self.shardsToBeSplited:

                if controller is None or shardAddr not in controller.shardsInfor:
                    sys.exit()
                shardTobeSplited = controller.shardsInfor[shardAddr]

                newShards = shardFactory.createSemanticShards(shardsNum=numOfSplitting)

                oldSubcorpus = shardTobeSplited.subCorpus
                oldBlocksList = shardTobeSplited.blocksList

                sizeOfSubcorpus = oldSubcorpus.shape[0]
                sizeOfBlocksList = len(oldBlocksList)
                if sizeOfSubcorpus != sizeOfBlocksList:
                    sys.exit()

                sizeOfEachSubcorpus = sizeOfSubcorpus // numOfSplitting

                for sequenceOfNewShard in range(numOfSplitting):

                    startPosition = sizeOfEachSubcorpus * sequenceOfNewShard
                    if sequenceOfNewShard == numOfSplitting - 1:

                        endPosition = None
                    else:
                        endPosition = sizeOfEachSubcorpus * sequenceOfNewShard + sizeOfEachSubcorpus

                    newShards[sequenceOfNewShard].addCorpus(oldSubcorpus[startPosition:endPosition])

                    oldBlocksList[startPosition].blockHeader.prevhash = "-1"
                    newShards[sequenceOfNewShard].addBlocks(oldBlocksList[startPosition:endPosition])

                for shard in newShards:

                controller.addShardInfor(newShards)

                controller.removeShardInfor(shardTobeSplited)

    def mergeShards(self, controller: BCController):

        self.isAfterMerged = True
        if self.shardsToBeMerged is None or len(self.shardsToBeMerged) == 0:
            return
        for shardAddr in self.shardsToBeMerged:

            if controller is None or shardAddr not in controller.shardsInfor:
                sys.exit()
            shardTobeMerged = controller.shardsInfor[shardAddr]
            nearestShard = controller.searchNearestShardByMeanCentroids(shardTobeMerged,
                                                                        shardsToBeSplited=self.shardsToBeSplited)

            if nearestShard is None:
                continue

            self.__mergeCorpusAndBlocks(shardMerged=shardTobeMerged, shardMerging=nearestShard)

            controller.shardsInfor.pop(shardAddr)

    def bindInclinedShardToNode(self, controller: BCController, smNodesDB: dict):

        if self.nodesInclinedShards is None:
            sys.exit()

        for addrAndInclinedShards in self.nodesInclinedShards:

            if addrAndInclinedShards is None:
                continue
            nodeAddr = addrAndInclinedShards[0]
            inclinedShards = addrAndInclinedShards[1]
            if nodeAddr not in smNodesDB:
                sys.exit()
            smNode = smNodesDB[nodeAddr]

            smNode.setNtEpInclinedShardAddr()
            for shardAddrAndTimes in inclinedShards:
                shardAddr = shardAddrAndTimes[0]
                if shardAddr in controller.shardsInfor:
                    smNode.setNtEpInclinedShardAddr(shardAddr)
                    break

    def __mergeCorpusAndBlocks(self, shardMerged, shardMerging):

        shardMerging.subCorpus = np.vstack((shardMerging.subCorpus, shardMerged.subCorpus))

        if shardMerged.blocksList is not None and shardMerging.blocksList is not None:
            shardMerged.blocksList[0].blockHeader.prevhash = shardMerging.blocksList[-1].blockHeader.blockAddr

        shardMerging.addBlocks(shardMerged.blocksList)

        return True

    def setShardsToBeSplited(self, shardsToBeSplited):

        self.shardsToBeSplited = list(set(shardsToBeSplited))
        print(f"将要分裂的切片为{self.shardsToBeSplited}")

    def setShardsToBeMerged(self, shardsToBeMerged):

        self.shardsToBeMerged = list(set(shardsToBeMerged))

    def setNodesInclinedShards(self, nodesInclinedShards):

        self.nodesInclinedShards = nodesInclinedShards

    def setCrossShardsTxs(self, tempShardsThroughput):

        shardAddrAndCrossShardsTxs = {}
        totalCrossShardTxs = 0
        for shardThrough in tempShardsThroughput:
            shardAddr = shardThrough[0][:10]
            crossShardTxs = shardThrough[1][1] + shardThrough[1][2]

            shardAddrAndCrossShardsTxs[shardAddr] = crossShardTxs
            totalCrossShardTxs += crossShardTxs

        self.shardAddrAndCrossShardsTxs = shardAddrAndCrossShardsTxs
        self.totalCrossShardTxs = totalCrossShardTxs

    def setInShardsTxs(self, tempShardsThroughput):

        shardAddrAndInShardsTxs = {}
        totalInShardTxs = 0
        for shardThrough in tempShardsThroughput:
            shardAddr = shardThrough[0][:10]
            inShardTxs = shardThrough[1][0]

            shardAddrAndInShardsTxs[shardAddr] = inShardTxs
            totalInShardTxs += inShardTxs

        self.shardAddrAndInShardsTxs = shardAddrAndInShardsTxs
        self.totalInShardTxs = totalInShardTxs
