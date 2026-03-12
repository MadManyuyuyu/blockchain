import faiss


class IndexFaiss:
    def __init__(
            self,
            vectorDim,
    ):
        self.vectorDim = vectorDim
        self.currentIndex = None
        self.ivfCentroids = None

    def buildIndexFlatIP(self):

        self.currentIndex = faiss.IndexFlatIP(self.vectorDim)
        return self.currentIndex

    def buildIndexIVFPQIP(self,
                          nlist,
                          nsegment,
                          nbit,
                          # directMap = False,
                          verbose=False,
                          OPQTimes=0
                          ):

        metric = faiss.METRIC_INNER_PRODUCT
        if OPQTimes > 0:
            OPQ = faiss.OPQMatrix(self.vectorDim, nsegment)
            OPQ.niter = OPQTimes

        quantizer = faiss.IndexFlatIP(self.vectorDim)

        self.currentIndex = faiss.IndexIVFPQ(
            quantizer, self.vectorDim, nlist, nsegment, nbit, metric
        )
        if OPQTimes > 0:
            self.currentIndex = faiss.IndexPreTransform(OPQ, self.currentIndex)
        return self.currentIndex

    def buildIndexIVFFlatIP(self,
                            nlist,
                            nsegment,
                            directMap=True,
                            verbose=False,
                            OPQTimes=0
                            ):

        metric = faiss.METRIC_INNER_PRODUCT
        if OPQTimes > 0:
            OPQ = faiss.OPQMatrix(self.vectorDim, nsegment)
            OPQ.niter = OPQTimes

        quantizer = faiss.IndexFlatIP(self.vectorDim)
        self.currentIndex = faiss.IndexIVFFlat(
            quantizer, self.vectorDim, nlist, metric
        )

        if OPQTimes > 0:
            self.currentIndex = faiss.IndexPreTransform(OPQ, self.currentIndex)
        return self.currentIndex

    def getIVFCentroids(self):

        self.ivfCentroids = self.currentIndex.quantizer.reconstruct_n(0, self.currentIndex.nlist)

        return self.ivfCentroids

    def trainData(self, data):

        self.currentIndex.train(data)
        return True

    def addData(self, data, directMap=True):

        self.currentIndex.add(data)
        if directMap is True:
            self.currentIndex.set_direct_map_type(faiss.DirectMap.Hashtable)


class IndexFactory:

    def __init__(self):
        pass

    def createdIndexFaiss(self, vectorDim):
        return IndexFaiss(vectorDim=vectorDim)
