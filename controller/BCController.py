
import numpy as np
from database.BCDatabase import NodesDB
from utils.calPart import getMeanCentroid


class BCController:

    def __init__(self,
                 indexFactory,
                 vectorDim,
                 semanticTool=None,

                 ):

        self.vectorDim = vectorDim
        self.indexFactory = indexFactory
        self.indexTool = None

        self.shardsIVFCentroids = None
        self.shadsIVFCentrosToShards = None

        self.semanticTool = semanticTool

        self.smNodesDB = None
        self.relyNodesDB = None
        self.committeeShard = None
        self.shardsInfor = None

        self.__setNodesDB()
        self.__setIndexTool()

    def __setNodesDB(self):

        self.smNodesDB = NodesDB()
        self.relyNodesDB = NodesDB()

    def __setIndexTool(self):

        self.indexTool = self.indexFactory.createdIndexFaiss(self.vectorDim)
        self.indexTool.buildIndexFlatIP()

    def setCommitteeShard(self, committeeShard):

        self.committeeShard = committeeShard
        return True

    def addShardInfor(self, shards):

        if self.shardsInfor is None:
            self.shardsInfor = {}

        if type(shards) == list:
            for shard in shards:
                self.shardsInfor[shard.shardAddr] = shard
        else:
            self.shardsInfor[shards.shardAddr] = shards

    def removeShardInfor(self, shards):

        if type(shards) == list:
            for shard in shards:
                result = self.shardsInfor.pop(shard.shardAddr, None)

        else:
            result = self.shardsInfor.pop(shards.shardAddr, None)

    def getNearestShards(self, semanticData, top=3):

        index_size = self.indexTool.currentIndex.ntotal
        top = index_size if index_size < 3 else top

        if not isinstance(semanticData, np.ndarray):
            semanticData = np.array(semanticData)
        if np.ndim(semanticData) != 2:
            semanticData = np.expand_dims(semanticData, axis=0)

        distances, indices = self.indexTool.currentIndex.search(semanticData, top)

        shardAddr = self.shadsIVFCentrosToShards[indices[0][0]]
        shard = self.shardsInfor[shardAddr]
        return shard

    def addShardsIVFCentroids(self, shardsIVFCentroids):

        self.shardsIVFCentroids = shardsIVFCentroids

    def addShadsIVFCentrosToShards(self, shadsIVFCentrosToShards):

        self.shadsIVFCentrosToShards = shadsIVFCentrosToShards

    def searchNearestShardByMeanCentroids(self, shardToBeMerged, shardsToBeSplited, topResults=15, ):

        ivfCentroids = shardToBeMerged.shardIndexTool.getIVFCentroids()

        meanCentroid = getMeanCentroid(ivfCentroids)
        meanCentroid = np.expand_dims(meanCentroid, axis=0)
        distances, indices = self.indexTool.currentIndex.search(meanCentroid, topResults)

        indices = indices[0]

        mergingShardAddr = None
        for index in indices:

            resultShardAddr = self.shadsIVFCentrosToShards[index]

            if resultShardAddr == shardToBeMerged.shardAddr:
                continue

            if resultShardAddr not in self.shardsInfor:
                print(f"切片地址{resultShardAddr}被融合了")

                continue

            if resultShardAddr in shardsToBeSplited:
                print(f"切片地址{resultShardAddr}是要被分裂的，不能被融合")
                continue

            mergingShardAddr = resultShardAddr
            break

        if mergingShardAddr is None:
            print(f"shardToBeMerged是{shardToBeMerged}")
            print("找不到执行合并的切片")
            return None

        return self.shardsInfor[mergingShardAddr]
