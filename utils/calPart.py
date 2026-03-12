
import random
import secrets
import sys
from collections import Counter
import numpy as np
def getRandomShardsPosition(nodeDBSize,shardsSize):

    baseSmNodeNum = nodeDBSize // shardsSize
    remainderSmNodeNum = nodeDBSize % shardsSize
    originalShardPositionList = list(range(shardsSize))
    extendShardPositionList = originalShardPositionList * baseSmNodeNum
    extendShardPositionList += originalShardPositionList[
                               :remainderSmNodeNum]
    np.random.shuffle(extendShardPositionList)
    return extendShardPositionList


def getLuckyDog(nodeNum):

    return random.randint(0, nodeNum-1)


def getNextNodeIndex(currentIndex,nodeNums):

    currentIndex+=1;
    return currentIndex % nodeNums

def getNodesInclinedShards(smNodesInitTxsHistory):

    if smNodesInitTxsHistory is None:
        return None
    nodeAdrrAndShardsAddr={}

    for nodeAddrNTargetAddr in smNodesInitTxsHistory:
        nodeAddr = nodeAddrNTargetAddr[0]
        targetAddr = nodeAddrNTargetAddr[1]

        if nodeAddr  not in nodeAdrrAndShardsAddr:
            nodeAdrrAndShardsAddr[nodeAddr] =[]
        nodeAdrrAndShardsAddr[nodeAddr].append(targetAddr)
    nodesAddrNInclinedShardsAddr=[]

    for nodeAddr,targetAddrs in nodeAdrrAndShardsAddr.items():

        counter = Counter(targetAddrs)
        if len(counter) < 3:
            topData = counter.most_common(len(counter))
        else:
            topData = counter.most_common(3)
        nodesAddrNInclinedShardsAddr.append((nodeAddr,topData))
    return nodesAddrNInclinedShardsAddr



def getMeanCentroid(centroids):

    mean_vector = np.mean(centroids, axis=0)
    return mean_vector


def isSimilar(list1,list2):

    if list1 is None or len(list1) == 0  or list2 is None or len(list2) == 0:
        return False
    return bool(set(list1)  &  set(list2))





def getRandomMeanSeats(nodeIndexList,shardNum):

    if len(nodeIndexList) < shardNum:
        print("语义节点数量少于切片数量")
        sys.exit()

    random.shuffle(nodeIndexList)

    positions = [[] for _ in range(shardNum)]


    for i, num in enumerate(nodeIndexList):
        positions[i % shardNum].append(num)

    return positions


def cosine_similarity(a, b):

    dot = np.dot(a, b)
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    return dot / (norm_a * norm_b)