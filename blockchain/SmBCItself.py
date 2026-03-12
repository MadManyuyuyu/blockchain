import sys
import time
from blockchain.Consensus.SemanticConsensus import CmShardConsensus, SmShardConsensus
from controller.BCController import BCController
from index4BC.IndexTool import IndexFactory
from utils.aboutTime import getCurrentTime
from blockchain.NodeFactory import NodeFactory
from blockchain.ShardStrategy.ShardingStrategies import SemanticStrategy, CommitteeStrategy
from database.BCDatabase import NodesDB
from blockchain.ShardFactory import ShardFactory
from blockchain.BCSecurity.CmSecurity import CmInforVerifier
import faiss
from utils.encodePart import getUniRawID
from utils.getRandomCorpusIndices import getCorpusIndiceDict
from utils.indexParms import getShardNlist, getShardNbit
import numpy as np
from utils.calPart import getRandomShardsPosition, getLuckyDog, isSimilar, getRandomMeanSeats, getNextNodeIndex
from blockchain.BlockFactory import BlockFactory
import random

from logs import logger, logExpResults


class SmBC:
    def __init__(self, hyperParms=None, corpus=None, groupIndices=None, numOfGroup=0, numOfInitialCorpus=0):
        self.corpus = corpus
        self.initCorpus = None
        self.runningCorpus = None
        self.queryCorpus = None
        self.hyperParms = hyperParms
        self.randomSeed = None
        self.groupIndices = groupIndices
        self.numOfGroup = numOfGroup
        self.nodeGroups = None
        self.numOfInitialCorpus = numOfInitialCorpus
        self.runningEpochs = None
        self.currentEpoch = None
        self.vectorDim = None
        self.shardsNum = None
        self.indexFactory = None
        self.nodeFactory = None
        self.shardFactory = None
        self.blockFactory = None
        self.BCController = None
        self.startTime = None
        self.initSmNodeNum = None
        self.initRlyNodeNum = None
        self.initCmNodeNum = None
        self.nodesLocations = None
        self.rateToHotShard = None
        self.shardingStrategy = None
        self.cmNodeDB = None
        self.smNodeDB = None
        self.rlyNodeDB = None
        self.cmVerifier = None
        self.cmShard = None
        self.initSmShards = None
        self.initShardsIndex = None
        self.newShardsIVFCentroids = None
        self.shardsIVFCentrosToShards = None
        self.smDataToNodeDict = None

    def run(self):
        self.startTime = getCurrentTime()
        self.initBC()
        self.ET930AM()

    def initBC(self):

        self.__readHyperParms()
        self.currentEpoch = 0
        random.seed(self.randomSeed)
        self.indexFactory = IndexFactory()
        self.nodeFactory = NodeFactory()
        self.blockFactory = BlockFactory()
        self.cmVerifier = CmInforVerifier()
        self.shardFactory = ShardFactory()
        self.__initNodes()
        self.__initShards()
        self.__initShardsIndex()
        self.BCController = BCController(indexFactory=self.indexFactory, vectorDim=self.vectorDim)
        self.__connectUnits()

    def ET930AM(self):

        self.cmShard.temCrossTxsNumList = []
        self.cmShard.temInTxsNumList = []
        self.cmShard.temRunningTime = []
        for currentEpoch in range(self.runningEpochs):
            self.currentEpoch = currentEpoch
            if currentEpoch == 0:
                epochCorpus, self.runningCorpus = self.__getEpochCorpus(self.runningCorpus, currentEpoch,                                                        self.runningEpochs)
            else:
                epochCorpus = self.__getEpochCorpus(self.runningCorpus, currentEpoch,
                                                    self.runningEpochs)
            epochCorpusIndices = getCorpusIndiceDict(groupIndices=self.groupIndices, numOfIndices=len(epochCorpus))
            self.cmShard.setTotalShardNum(
                shardsNum=len(self.BCController.shardsInfor))
            self.cmShard.temCrossTxsNum = 0;
            for row in range(len(epochCorpus)):
                typeAndIndice = getCorpusIndiceDict(groupIndices=epochCorpusIndices, numOfIndices=1)
                if len(typeAndIndice) == 0:
                    break
                typeOfEmbedding = None
                indiceOfEmbedding = None
                for k, v in typeAndIndice.items():
                    typeOfEmbedding = k
                    indiceOfEmbedding = v
                if typeOfEmbedding is None:
                    print(typeAndIndice)
                uploadNode = self.__getMinerFromG(typeOfEmbedding, num=1)
                embeddingToUpload = self.corpus[indiceOfEmbedding]
                uploadNode.uploadTxs(semanticData=embeddingToUpload, controller=self.BCController,
                                     cmShard=self.cmShard)
                self.__markSmDataToNode(node=uploadNode, semanticData=embeddingToUpload)

            self.cmShard.temCrossTxsNumList.append(self.cmShard.temCrossTxsNum)
            self.cmShard.temInTxsNumList.append(len(epochCorpus) - self.cmShard.temCrossTxsNum)
            actualUpload = 0
            for shard in self.BCController.shardsInfor.values():
                actualUpload = actualUpload + (0 if shard.temSmTxsPool is None else len(shard.temSmTxsPool))

            for smShardIndex, smShard in enumerate(self.BCController.shardsInfor.values()):
                numOfBlocksAdded = smShard.letUsRockNRoll()
                inspectingCmNode = smShard.committeeNode
                inspectingCmNode.smNodesActionsInfor = smShard.tempInforToCmShard
                inspectingCmNode.handleNodesActionsToCmShard(self.cmShard)

            self.cmShard.setConsensusTactic(CmShardConsensus(rateToHotShard=self.rateToHotShard))
            self.getTraces(list(self.BCController.shardsInfor.values()))
            self.cmShard.letUsRockNRoll()
            if self.cmShard is None or self.cmShard.blocksList is None:
                sys.exit()
            else:
                cmBlock = self.cmShard.blocksList[-1]
                shardingStrategy = cmBlock.shardingStrategy

                similarResult = isSimilar(shardingStrategy.shardsToBeSplited, shardingStrategy.shardsToBeMerged)
                if similarResult is True:
                    sys.exit()

                shardingStrategy.mergeShards(controller=self.BCController)
                shardingStrategy.splitShards(shardFactory=self.shardFactory,
                                             controller=self.BCController,
                                             numOfSplitting=2)

                shardingStrategy.bindInclinedShardToNode(controller=self.BCController,
                                                         smNodesDB=self.smNodeDB.bcNodesDB)

            epochEndTime = time.time()

            self.cmShard.temRunningTime.append(round((epochEndTime - epochStartTime), 6))

            self.__prepareForNextEpoch()
    def __markSmDataToNode(self, node, semanticData):
        smallData = getUniRawID(semanticData=semanticData)
        self.smDataToNodeDict[smallData] = node

    def __getEpochCorpus(self, runningCorpus, currentEpoch, runningEpochs):

        numOfRunningCorpus = len(runningCorpus)
        if currentEpoch == 0:
            y = numOfRunningCorpus * 0.3
            y = int(y)
            firstCorpus = runningCorpus[:y]
            runningCorpus = runningCorpus[y:]
            return firstCorpus, runningCorpus
        eachNum = numOfRunningCorpus // runningEpochs
        lastRemainder = numOfRunningCorpus % runningEpochs
        startPosition = currentEpoch * eachNum
        endPosition = startPosition + eachNum - 1
        if currentEpoch + 1 == runningEpochs:
            endPosition += lastRemainder
        return runningCorpus[startPosition:endPosition]

    def __readHyperParms(self):

        self.vectorDim = self.hyperParms["vectorDim"]
        self.runningEpochs = self.hyperParms["runningEpochs"]
        self.initShardsNum = self.hyperParms["initShardsNum"]
        self.initNsegment = self.hyperParms["initNsegment"]
        self.initSmNodeNum = self.hyperParms["initSmNodeNum"]
        self.initRlyNodeNum = self.hyperParms["initRlyNodeNum"]
        self.initCmNodeNum = self.hyperParms["initCmNodeNum"]
        self.rateToHotShard = self.hyperParms["rateToHotShard"]
        self.nodesLocations = self.hyperParms["nodesLocations"]
        self.initCorpus = self.hyperParms["initCorpus"]
        self.runningCorpus = self.hyperParms["runningCorpus"]
        self.queryCorpus = self.hyperParms["queryCorpus"]
        self.randomSeed = self.hyperParms["randomSeed"]

    def __initNodes(self):

        self.cmNodeDB = NodesDB()
        self.smNodeDB = NodesDB()
        self.rlyNodeDB = NodesDB()
        lenLocations = len(self.nodesLocations)
        numOfCmNodeForLocation = numOfCmNodeForEach
        numOfSmNodeForLocation = numOfSmNodeForEach
        numOfRlyNodeForLocation = numOfRlyNodeForEach

        newCmNodes = self.nodeFactory.createdCmNodesByLocation(location=location, nodeNum=numOfCmNodeForLocation)
        newSmNodes = self.nodeFactory.createdSmNodesByLocation(location=location, nodeNum=numOfSmNodeForLocation,
                                                               hashRate=0)
        newRlyNodes = self.nodeFactory.createdRlyNodesByLocation(location=location, nodeNum=numOfRlyNodeForLocation,
                                                                 controller=self.BCController)

        if type(newCmNodes) == list:
            self.cmNodeDB.putNodes(newCmNodes)
        self.smNodeDB.putNodes(newSmNodes)
        self.rlyNodeDB.putNodes(newRlyNodes)
        else:
        self.cmNodeDB.putNode(newCmNodes.nodeAddr, newCmNodes)
        self.smNodeDB.putNode(newSmNodes.nodeAddr, newSmNodes)
        self.rlyNodeDB.putNode(newRlyNodes.nodeAddr, newRlyNodes)

        self.__logInstances(list(self.cmNodeDB.bcNodesDB.values()))
        self.__logInstances(list(self.smNodeDB.bcNodesDB.values()))
        self.__logInstances(list(self.rlyNodeDB.bcNodesDB.values()))

    def __initShards(self):

        self.cmShard = self.shardFactory.createCommitteeShard(self.cmVerifier)
        self.initSmShards = self.shardFactory.createSemanticShards(shardsNum=self.initShardsNum)

        self.__logInstances([self.cmShard])

        self.__logInstances(self.initSmShards)

    def __initShardsIndex(self):

        temIndex = self.indexFactory.createdIndexFaiss(self.vectorDim)
        self.initShardsIndex = temIndex
        self.initShardsIndex.buildIndexIVFFlatIP(nlist=self.initShardsNum, nsegment=self.initNsegment)

        self.initShardsIndex.trainData(self.initCorpus)

        self.initShardsIndex.addData(self.initCorpus, directMap=True)

    def __connectUnits(self):

        index = self.initShardsIndex.currentIndex
        numOfShardsIndexAllocated = 0
        for list_no in range(index.nlist):

            size = index.invlists.list_size(list_no)

            if size != 0:
                ids_ptr = index.invlists.get_ids(list_no)
                ids = faiss.rev_swig_ptr(ids_ptr, size)
                vectors = index.reconstruct_batch(ids)
                self.initSmShards[numOfShardsIndexAllocated].addCorpus(vectors=vectors)
                numOfShardsIndexAllocated += 1

        self.__shardsIndexUnit()

        self.BCController.indexTool.trainData(self.newShardsIVFCentroids)
        self.BCController.indexTool.addData(self.newShardsIVFCentroids, directMap=False)
        self.BCController.addShardsIVFCentroids(self.newShardsIVFCentroids)
        self.BCController.addShadsIVFCentrosToShards(self.shardsIVFCentrosToShards)
        self.BCController.addShardInfor(self.initSmShards)

        cmNodes = list(self.cmNodeDB.bcNodesDB.values())
        self.cmShard.addNodesToShard(cmNodes)
        for cmNode in cmNodes:
            cmNode.setNodeBelongingChainAddr(self.cmShard.shardAddr)

        smNodesDBSize = self.smNodeDB.returnNodeDBSize()

        extendShardPositionList = getRandomShardsPosition(nodeDBSize=smNodesDBSize, shardsSize=self.initShardsNum)

        if len(extendShardPositionList) != self.smNodeDB.returnNodeDBSize():
            sys.exit()

        self.__nodeAndShardUnit(self.smNodeDB.bcNodesDB, extendShardPositionList, self.initSmShards)

        if self.numOfGroup != 0:

            keys_to_remove = []
            for key in self.groupIndices:
                self.groupIndices[key] = [x for x in self.groupIndices[key] if x >= self.numOfInitialCorpus]
                if len(self.groupIndices[key]) == 0:
                    keys_to_remove.append(key)
                self.numOfGroup = len(self.groupIndices)

            for key in keys_to_remove:
                self.groupIndices.pop(key)

            totalCount = 0
            for k, v in self.groupIndices.items():
                totalCount += len(self.groupIndices[k])
            if totalCount > len(self.runningCorpus):
                sys.exit()

            lenOfNodeGroup = self.initSmNodeNum // self.numOfGroup

            self.nodeGroups = {}

            nodeAddress = list(self.smNodeDB.bcNodesDB.keys())
            random.shuffle(nodeAddress)
            groupStartIndice = 0
            for groupNum in range(self.numOfGroup):
                groupEndIndice = groupStartIndice + lenOfNodeGroup
                groupNum = str(groupNum)
                self.nodeGroups[groupNum] = []
                if int(groupNum) + 1 == self.numOfGroup:
                    self.nodeGroups[groupNum] = nodeAddress[groupStartIndice:]
                else:
                    self.nodeGroups[groupNum] = nodeAddress[groupStartIndice:groupEndIndice]
                groupStartIndice = groupEndIndice

            for k, v in self.nodeGroups.items():
                for nodeAddr in v:
                    smNode = self.smNodeDB.bcNodesDB[nodeAddr]
                    if smNode.groupNum is not None:
                        sys.exit()
                    smNode.groupNum = k

        rlyNodesDBSize = self.rlyNodeDB.returnNodeDBSize()
        extendShardPositionListForRly = getRandomShardsPosition(nodeDBSize=rlyNodesDBSize,
                                                                shardsSize=self.initShardsNum)

        self.__nodeAndShardUnit(self.rlyNodeDB.bcNodesDB, extendShardPositionListForRly, self.initSmShards)

        for shard in self.initSmShards:

            shard.setBlockFactory(blockFactory=self.blockFactory)
            if shard.subCorpus is not None:
                self.__initShardsChain(shard)

        if len(self.cmNodeDB.bcNodesDB) < len(self.initSmShards):

            sys.exit()

        else:

            cmNodesList = list(self.cmNodeDB.bcNodesDB.values())
            self.__randAssignCmNodesToShards(cmNodesList, self.initSmShards)

        self.smDataToNodeDict = {}

    def __nodeAndShardUnit(self, nodesDB, extendShardPositionList, shardsList):

        temShardPosition = 0
        for nodeAddr, node in nodesDB.items():
            shardPosition = extendShardPositionList[temShardPosition]
            shard = shardsList[shardPosition]
            shard.addNodesToShard(node)
            node.setNodeBelongingChainAddr(shard.shardAddr)
            temShardPosition += 1

    def __shardsIndexUnit(self):

        initShardsNoVectors = []

        self.shardsIVFCentrosToShards = {}

        centroidsStartPosition = 0

        for indexNum, shard in enumerate(self.initSmShards):
            shard.shardIndexTool = self.indexFactory.createdIndexFaiss(
                self.vectorDim)
            if shard.subCorpus is None:
                initShardsNoVectors.append(shard)
                continue
            shardNlist = getShardNlist(
                shard.subCorpus.shape[0])
            shardNbit = getShardNbit(shardCorpusSize=shard.subCorpus.shape[0],
                                     nsegment=self.initNsegment)

            shard.shardIndexTool.buildIndexIVFFlatIP(nlist=shardNlist, nsegment=self.initNsegment)

            shard.shardIndexTool.trainData(data=shard.subCorpus)
            shard.shardIndexTool.addData(data=shard.subCorpus,
                                         directMap=True)

            shardIVFCentroids = shard.shardIndexTool.getIVFCentroids()

            if self.newShardsIVFCentroids is None:
                self.newShardsIVFCentroids = []
                self.newShardsIVFCentroids.append(shardIVFCentroids)
                self.newShardsIVFCentroids = np.vstack(
                    self.newShardsIVFCentroids)
            else:
                self.newShardsIVFCentroids = np.vstack((self.newShardsIVFCentroids, shardIVFCentroids))
            centroidEndPosition = centroidsStartPosition + shardIVFCentroids.shape[0] - 1

            while centroidsStartPosition <= centroidEndPosition:
                self.shardsIVFCentrosToShards[centroidsStartPosition] = shard.shardAddr
                centroidsStartPosition += 1

    def __initShardsChain(self, smShard):

        bFactory = smShard.blockFactory
        prevBlockHash = None

        newBlocks = []
        for row in range(smShard.subCorpus.shape[0]):

            headData = {}
            txData = {}
            if row == 0:

                headData["prevhash"] = "-1"
            else:
                headData["prevhash"] = prevBlockHash

            headData["minerAddr"] = "0"
            headData["difficulty"] = -1
            headData["committeeNodeAddr"] = "0"
            txData["sender"] = "0"
            txData["receiver"] = "0"
            txData["signature"] = "lanaLee"
            txData["semanticData"] = smShard.subCorpus[row]

            newBlock = bFactory.getBlock(headerData=headData, txsData=[txData])
            prevBlockHash = newBlock.blockHeader.blockAddr
            newBlocks.append(newBlock)

        smShard.addBlocks(newBlocks)

    def __randAssignCmNodesToShards(self, cmNodes, smShards):

        numCmNodes = len(cmNodes)
        numSmShards = len(smShards)
        cmNodesPositions = random.sample(list(range(numCmNodes)), numSmShards)

        for cmNodePosition, smShard in zip(cmNodesPositions, smShards):
            cmNode = cmNodes[cmNodePosition]
            smShard.setCmNode(cmNode)

    def __prepareForNextEpoch(self):

        self.__resetUnits()
        self.__updateUnits()

    def __resetUnits(self):

        self.cmShard.consensusTactic = None
        self.cmShard.shardsActionsInforPool = None

        for smShard in self.BCController.shardsInfor.values():
            smShard.committeeNode = None
            smShard.smNodesInfor = None
            smShard.rlyNodes = None
            smShard.tempInforToCmShard = None
            smShard.shardIndexTool = None
            smShard.temSmTxsPool = None
            smShard.consensusTactic = None

        for cmNode in self.cmNodeDB.bcNodesDB.values():
            cmNode.inspectingShardAddr = None
            cmNode.smNodesActionsInfor = None

        for smNode in self.smNodeDB.bcNodesDB.values():
            smNode.belongingChainAddr = None

        for rlyNode in self.rlyNodeDB.bcNodesDB.values():
            rlyNode.belongingChainAddr = None

        self.BCController.indexTool.currentIndex.reset()
        self.BCController.shardsIVFCentroids = None
        self.BCController.shadsIVFCentrosToShards = None

        self.initSmShards = None
        self.initShardsIndex = None
        self.newShardsIVFCentroids = None
        self.shardsIVFCentrosToShards = None

    def __updateUnits(self):

        smShardingStrategy = self.__getLatestStrategy()

        smNodes = list(self.smNodeDB.bcNodesDB.values())
        self.__bindSmNodesAndShards(smNodes=smNodes, controller=self.BCController,
                                    smShardingStrategy=smShardingStrategy)

        rlyNodes = list(self.rlyNodeDB.bcNodesDB.values())
        self.__bindRlyNodesAndShards(rlyNodes=rlyNodes, controller=self.BCController)

        self.__bindCmNodesAndShards(cmNodes=list(self.cmNodeDB.bcNodesDB.values()), controller=self.BCController)

        smShards = list(self.BCController.shardsInfor.values())

        shardsIVFCentroidsMatrix, shardsIVFCentrosToShards = self.__trainIndexWithSubcorpus(shards=smShards)

        self.__bindSmConsensus(smShards=smShards)

        for smShard in smShards:
            smShard.setBlockFactory(self.blockFactory)

        self.BCController.shardsIVFCentroids = shardsIVFCentroidsMatrix
        self.BCController.shadsIVFCentrosToShards = shardsIVFCentrosToShards
        self.BCController.indexTool.trainData(shardsIVFCentroidsMatrix)
        self.BCController.indexTool.addData(shardsIVFCentroidsMatrix, directMap=False)

    def __getLatestStrategy(self):

        if self.cmShard and self.cmShard.blocksList:
            latestCmBlock = self.cmShard.blocksList[-1]
        else:

            sys.exit()
        smShardingStrategy = latestCmBlock.shardingStrategy
        if smShardingStrategy is None:
            sys.exit()
        return smShardingStrategy

    def __bindSmNodesAndShards(self, smNodes: list, controller: BCController, smShardingStrategy: SemanticStrategy):

        toHotShardRate = smShardingStrategy.rateToHotShard
        numOfSmNodes = len(smNodes)
        numOfSmShards = len(controller.shardsInfor)
        if numOfSmNodes < numOfSmShards:
            sys.exit()

        leastNumSmNode = numOfSmNodes // numOfSmShards
        notToHotShard = set()
        for index, smNode in enumerate(smNodes):
            inclinedShardAddr = smNode.nextEpochInclinedShardAddr
            rateToNonHotShard = random.random()
            if inclinedShardAddr is None or toHotShardRate < rateToNonHotShard:
                notToHotShard.add(index)
                continue

            if inclinedShardAddr and inclinedShardAddr not in controller.shardsInfor:
                notToHotShard.add(index)
                continue

            inclinedShard = controller.shardsInfor[inclinedShardAddr]
            if inclinedShard.smNodesInfor is not None and len(inclinedShard.smNodesInfor) >= leastNumSmNode:
                notToHotShard.add(index)
                continue
            self.__loveEachOther(node=smNode, shard=inclinedShard)

        allocatedPosition = getRandomMeanSeats(nodeIndexList=list(notToHotShard), shardNum=numOfSmShards)

        for position, shard in enumerate(self.BCController.shardsInfor.values()):

            nodeIndexs = allocatedPosition[position]
            for index in nodeIndexs:
                self.__loveEachOther(smNodes[index], shard)

    def __loveEachOther(self, node, shard):

        shard.addNodesToShard(node)
        node.setNodeBelongingChainAddr(shard.shardAddr)

    def __bindRlyNodesAndShards(self, rlyNodes: list, controller: BCController):

        numOfRlyNodes = len(rlyNodes)
        numOfSmShards = len(controller.shardsInfor)

        if numOfRlyNodes < numOfSmShards:
            sys.exit()

        allocatedPosition = getRandomMeanSeats(nodeIndexList=list(range(numOfRlyNodes)), shardNum=numOfSmShards)

        for position, shard in enumerate(self.BCController.shardsInfor.values()):

            nodeIndexs = allocatedPosition[position]
            for index in nodeIndexs:
                self.__loveEachOther(rlyNodes[index], shard)

    def __bindCmNodesAndShards(self, cmNodes, controller):

        smShards = list(controller.shardsInfor.values())
        if len(cmNodes) < len(smShards):
            sys.exit()
        assignedCmNodes = random.sample(cmNodes, len(smShards))
        for currentNum, smShard in enumerate(smShards):
            assignedCmNode = assignedCmNodes[currentNum]
            smShard.setCmNode(assignedCmNode)

    def __bindSmConsensus(self, smShards: list, smConsensus=None):

        for shard in smShards:
            shard.setConsensusTactic(SmShardConsensus())

    def __trainIndexWithSubcorpus(self, shards: list):

        shardsIVFCentroidsMatrix = None
        shardsIVFCentrosToShards = {}

        centroidsStartPosition = 0
        for shard in shards:
            shard.shardIndexTool = self.indexFactory.createdIndexFaiss(
                self.vectorDim)
            if shard.subCorpus is None:
                sys.exit()
            shardNlist = getShardNlist(
                shard.subCorpus.shape[0])
            shardNbit = getShardNbit(shardCorpusSize=shard.subCorpus.shape[0],
                                     nsegment=self.initNsegment)

            shard.shardIndexTool.buildIndexIVFFlatIP(nlist=shardNlist, nsegment=self.initNsegment)

            shard.shardIndexTool.trainData(data=shard.subCorpus)
            shard.shardIndexTool.addData(data=shard.subCorpus,
                                         directMap=True)

            shardIVFCentroids = shard.shardIndexTool.getIVFCentroids()

            if shardsIVFCentroidsMatrix is None:
                shardsIVFCentroidsMatrix = []
                shardsIVFCentroidsMatrix.append(shardIVFCentroids)
                shardsIVFCentroidsMatrix = np.vstack(
                    shardsIVFCentroidsMatrix)
            else:
                shardsIVFCentroidsMatrix = np.vstack((shardsIVFCentroidsMatrix, shardIVFCentroids))
            centroidEndPosition = centroidsStartPosition + shardIVFCentroids.shape[0] - 1

            while centroidsStartPosition <= centroidEndPosition:
                shardsIVFCentrosToShards[centroidsStartPosition] = shard.shardAddr
                centroidsStartPosition += 1
        return shardsIVFCentroidsMatrix, shardsIVFCentrosToShards

    def __logInstances(self, instances):
        pass
    def getQueryResults(self, queryEmbedding, times=0, topResults=3):

        if times != 0:
            topResults = topResults * times
        queryResults = {}

        if type(queryEmbedding) != np.ndarray:
            queryEmbedding = np.ndarray(queryEmbedding)

        if queryEmbedding.ndim != 2:
            queryEmbedding = np.expand_dims(queryEmbedding, axis=0)

        controllerIndex = self.BCController.indexTool.currentIndex

        distances, indices = controllerIndex.search(queryEmbedding, topResults)

        for queryIndex, matrixIndices in enumerate(indices):

            ivfToShardAddrDict = self.BCController.shadsIVFCentrosToShards

            shardAddrs = list(map(ivfToShardAddrDict.get, matrixIndices))

            uniShardAddrs = list(dict.fromkeys(shardAddrs))

            eachQueryEmbedding = np.expand_dims(queryEmbedding[queryIndex], axis=0)

            corpusList = []
            distancesLits = []
            for shardsIndex, uniShardAddr in enumerate(uniShardAddrs):

                smShard = self.BCController.shardsInfor[uniShardAddr]
                smShardIndex = smShard.shardIndexTool.currentIndex

                eachQueryEmbeddingDs, eachQueryEmbeddingIs = smShardIndex.search(eachQueryEmbedding, topResults)
                distancesLits += eachQueryEmbeddingDs[0].tolist()
                shardEmbedding = smShard.subCorpus

                for eachQueryEmbeddingI in eachQueryEmbeddingIs[0]:
                    resultEmbedding = shardEmbedding[eachQueryEmbeddingI]
                    corpusList.append(resultEmbedding)

            paired = sorted(zip(distancesLits, corpusList), key=lambda distancesLits: distancesLits[0], reverse=True)

            a_sorted, b_sorted = zip(*paired)

            a_sorted = list(a_sorted)

            corpusList = list(b_sorted)

            queryResults[queryIndex] = corpusList

        return queryResults

    def getTraces(self, shards):

        for shard in shards:
            if shard.tempInforToCmShard is None:
                print(f"切片{shard.shardAddr} 没有信息体")

    def __getMinerFromG(self, typeOfEmbedding, num=1):

        nodeGroup = self.nodeGroups[typeOfEmbedding]
        if num > 1:
            sys.exit()
        minerAddr = random.sample(nodeGroup, num)
        miner = self.smNodeDB.bcNodesDB[minerAddr[0]]
        if miner is None:
            sys.exit()
        return miner
