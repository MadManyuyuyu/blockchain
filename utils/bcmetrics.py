
import numpy as np
def evalRecall(hitNum,totalNum):

    return  hitNum/totalNum

def evalPrecision(hitNum,totalNum):
    return hitNum / totalNum


def evalNDCG():
    pass

def __evalAP(searchNums,hitPositionsList):

    apResultsSum = 0
    for searchNum in range(searchNums):
        hitPosition = hitPositionsList[searchNum]
        if hitPosition > 0:
            apResult = 1 / hitPosition
        else:
            apResult = 0
        apResultsSum += apResult
    return apResultsSum

def evalMAP(searchNums,hitPositionsList):
    apResultsSum = __evalAP(searchNums,hitPositionsList)
    return  apResultsSum / searchNums

def evalCrossTxsRatio(csTxsNum,totalTxsNum):
    return csTxsNum / totalTxsNum


def calResponse(timeWaited,timeProcessed):

    return timeWaited + timeProcessed

import numpy as np

def dcg_at_k(relevance_scores, resultNum=9):
    k = min(len(relevance_scores), resultNum)
    return np.sum(relevance_scores[:k] / np.log2(np.arange(2, k + 2)))

def ndcg_at_k(similaritiesList, similarThreshold, resultNum=9):
    similaritiesList = np.array(similaritiesList)
    relevance_scores = np.where(similaritiesList > similarThreshold, 1, 0)

    k = min(len(relevance_scores), resultNum)

    dcg = dcg_at_k(relevance_scores, k)
    ideal_relevance = sorted(relevance_scores, reverse=True)
    idcg = dcg_at_k(ideal_relevance, k)

    return dcg / idcg if idcg > 0 else 0

def mean_ndcg(similarity_matrix, similarThreshold, resultNum=9):
    ndcg_scores = []
    for similarities in similarity_matrix:
        if len(similarities) < resultNum:
            print(f"Warning: Expected at least {resultNum} results, but got {len(similarities)}")
        ndcg_scores.append(ndcg_at_k(similarities, similarThreshold, resultNum))
    return np.mean(ndcg_scores)


