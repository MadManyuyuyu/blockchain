import random
import sys
from abc import abstractmethod

from blockchain.BCSecurity.CmSecurity import CmInforVerifier
from blockchain.Message.InforZip import TempSmShardInfor
from blockchain.ShardStrategy.ShardingStrategies import SemanticStrategy
from blockchain.Transcation.SemanticTX import SemanticTransaction
from utils.calPart import getNodesInclinedShards


class BaseConsensus:

    def __init__(self):

    @abstractmethod
    def executeConsensus(self, **agr):
        pass


class CmShardConsensus(BaseConsensus):

    def __init__(self, rateToHotShard=0.6):
        super().__init__()

        self.rateToHotShard = rateToHotShard

    def executeConsensus(self,
                         cmVerifier: CmInforVerifier,
                         shardsActionsInforPool: list
                         ):

        isLegitimate = cmVerifier.verifyInfor(cmTemInforPool=shardsActionsInforPool)

        if isLegitimate is False:
            sys.exit()

        tempShardsThroughput = self.__getShardsThroughput(shardsActionsInforPool)
        nodesInclinedShards = self.__getNodesInclinedShards(shardsActionsInforPool)
        shardsToBeSplited = self.__getShardsToBeSplited(tempShardsThroughput)
        shardsToBeMerged = self.__getShardsToBeMerged(tempShardsThroughput)
        smShardsShardingStrategy = SemanticStrategy(rateToHotShard=self.rateToHotShard)
        smShardsShardingStrategy.setShardsToBeSplited(shardsToBeSplited)
        smShardsShardingStrategy.setShardsToBeMerged(shardsToBeMerged)
        smShardsShardingStrategy.setNodesInclinedShards(nodesInclinedShards)
        smShardsShardingStrategy.setCrossShardsTxs(tempShardsThroughput=tempShardsThroughput)
        smShardsShardingStrategy.setInShardsTxs(tempShardsThroughput=tempShardsThroughput)

        return smShardsShardingStrategy

    def __getShardsThroughput(self, shardsActionsInforPool: list[TempSmShardInfor]):

        shardsThroughput = []
        for shardInfor in shardsActionsInforPool:
            if shardInfor is None:
                continue
            inshardTP = shardInfor.inShardTxsNum
            sendoutTP = shardInfor.sendOutTxsNum
            receiveTP = shardInfor.takeInTxsNum

            shardThroughput = shardInfor.inShardTxsNum + shardInfor.sendOutTxsNum
            shardsThroughput.append((shardInfor.shardAddr, [inshardTP, sendoutTP, receiveTP, shardThroughput]))
        return shardsThroughput

    def __getShardsToBeSplited(self, tempShardsThroughput):

        shardsToBeSplited = []
        totalBlocks = 0
        shardsNewBlocksNum = []
        for shardThrough in tempShardsThroughput:
            shardBlocksNum = shardThrough[1][0] + shardThrough[1][2]
            totalBlocks += shardBlocksNum
            shardsNewBlocksNum.append(shardBlocksNum)

        for index, shardBlocksNum in enumerate(shardsNewBlocksNum):

            if totalBlocks == 0:
                print("没有任何新区块上链")
                continue
            if shardBlocksNum / totalBlocks >= 0.4:
                shardsToBeSplited.append(tempShardsThroughput[index][0])
        return shardsToBeSplited

    def __getShardsToBeMerged(self, tempShardsThroughput):

        shardsToBeMerged = []
        totalBlocks = 0
        shardsNewBlocksNum = []
        for shardThrough in tempShardsThroughput:
            shardBlocksNum = shardThrough[1][0] + shardThrough[1][2]
            totalBlocks += shardBlocksNum
            shardsNewBlocksNum.append(shardBlocksNum)

        for index, shardBlocksNum in enumerate(shardsNewBlocksNum):

            if totalBlocks == 0:
                print("没有任何新区块上链")
                continue
            if shardBlocksNum / totalBlocks <= 0.1:
                shardsToBeMerged.append(tempShardsThroughput[index][0])

        return shardsToBeMerged

    def __getNodesInclinedShards(self, shardsActionsInforPool):

        nodesInclinedShards = []
        for shardInfor in shardsActionsInforPool:

            if shardInfor is not None:
                smNodesInitTxsHistory = shardInfor.smNodesInitTxsHistory
                nodesAddrNInclinedShardsAddr = getNodesInclinedShards(smNodesInitTxsHistory)
                if nodesAddrNInclinedShardsAddr is None:
                    nodesAddrNInclinedShardsAddr = []
            else:
                nodesAddrNInclinedShardsAddr = []
            nodesInclinedShards.extend(nodesAddrNInclinedShardsAddr)
        return nodesInclinedShards


class SmShardConsensus(BaseConsensus):
    def __init__(self):
        super().__init__()
        self.ruleOfSelectingNodePacking = None

    def executeConsensus(self,
                         smNodes,
                         blockFactory,
                         tempTxsPool: list,
                         lastBlock,
                         cmNodeAddr,
                         difficulty=-1):

        if lastBlock is None:
            lastBlockAddr = None
        else:
            lastBlockAddr = lastBlock.blockHeader.blockAddr
        newBlocks = []

        if tempTxsPool is None:
            print("没有交易池")

        isTxsLegitimate = self.__isTxsLegitimate(tempTxsPool)
        if isTxsLegitimate is False:
            print("交易不合法")
            sys.exit()
        for txs in tempTxsPool:

            while True:
                nodeForPacking = random.choice(smNodes)
                if nodeForPacking.nodeAddr.startswith("rly"):
                    continue
                break
            if len(newBlocks) != 0:
                lastBlockAddr = newBlocks[-1].blockHeader.blockAddr

            smBlock = nodeForPacking.packAndProduceBlock(txs, blockFactory, lastBlockAddr, cmNodeAddr, difficulty)

            newBlocks.append(smBlock)

        isBlocksLegitimate = self.__isBlocksLegitimate(lastBlock, newBlocks)
        if isBlocksLegitimate is False:
            print("新区块生成不合法")
            sys.exit()

        return newBlocks

    def __isTxsLegitimate(self, txs):

        if type(txs) == list:

            for tx in txs:
                if type(tx) == list:

                    isLegitimate = self.__isTxsLegitimate(tx)
                    if not isLegitimate:
                        return False
                else:

                    if tx and tx.intoShardPoolTime is not None and tx.createdTime is not None:

                        if int(tx.intoShardPoolTime["time"]) < int(tx.createdTime):
                            return False
                    else:

                        return False

        else:

            if txs and txs.intoShardPoolTime is not None and txs.createdTime is not None:
                if int(txs.intoShardPoolTime["time"]) < int(txs.createdTime):
                    return False
            else:
                return False
        return True

    def __isBlocksLegitimate(self, lastBlock, blocks: list):

        preBlockTime = None

        for index, block in enumerate(blocks):

            nextBlockTime = block.blockHeader.timestamp
            if index == 0 and lastBlock is not None:
                preBlockTime = lastBlock.blockHeader.timestamp
            if int(preBlockTime) > int(nextBlockTime):
                return False
            preBlockTime = nextBlockTime
        return True
    def setSelectNodeRule(self, selectingRule):

        self.ruleOfSelectingNodePacking = selectingRule
