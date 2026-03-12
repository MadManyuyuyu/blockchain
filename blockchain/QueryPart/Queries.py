import sys

import numpy as np

import utils.bcmetrics as bcm
from utils.calPart import cosine_similarity


class Queries:
    def __init__(self, bc):
        self.bc = bc

    def testWithOldCorpus(self, testCorpus, checkResults, shardTopResult):

        times = 2
        results = self.bc.getQueryResults(queryEmbedding=testCorpus, times=times, topResults=shardTopResult)

        answerNum = 0;
        for rlist in results.values():
            answerNum += len(rlist)

        similarThresh = 0.7
        accTopK = 5

        accCount = 0;
        similarCount = 0;
        apPositionList = [-1] * len(testCorpus)
        similarMatirx = []

        for qIndex, queryResult in results.items():
            queryE = testCorpus[qIndex]
            for rEmbedding in queryResult:
                if np.allclose(queryE, rEmbedding) or cosine_similarity(queryE, rEmbedding) >= similarThresh:
                    similarCount += 1

        expectedTOPK = shardTopResult ** 2
        for qIndex, queryResult in results.items():
            queryE = testCorpus[qIndex]
            similarList = []
            queryResult = queryResult[:expectedTOPK]
            for rIndex, rEmbedding in enumerate(queryResult):

                if np.allclose(queryE, rEmbedding):
                    similarList.append(1)
                    if apPositionList[qIndex] == -1:
                        apPositionList[qIndex] = rIndex + 1
                    else:

                        pass
                if np.allclose(queryE, rEmbedding) and rIndex < accTopK:
                    accCount += 1
                else:
                    similarity = cosine_similarity(queryE, rEmbedding)
                    similarList.append(similarity)
            similarMatirx.append(similarList)


