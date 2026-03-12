


import math
import numpy as np
def getShardNlist(shardCorpusSize):

    if shardCorpusSize == 0:
        return 0
    sqrt_x = math.sqrt(shardCorpusSize)
    nlist = math.ceil(sqrt_x)
    return  nlist


def getShardNbit(shardCorpusSize,nsegment):

    if shardCorpusSize == 0:
        return 0
    sharNbit = int(np.log2(shardCorpusSize/nsegment * 2))
    sharNbit = sharNbit if sharNbit >= 4 else 0
    return sharNbit




