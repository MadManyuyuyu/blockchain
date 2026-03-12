from abc import abstractmethod

from utils.encodePart import encode_hex, keccak_256
from blockchain.Transcation.SemanticTX import SemanticTransaction
from utils.aboutTime import getCurrentTime
from utils.getUniAddr import returnUniAddr


class BlockHeader:
    def __init__(self,
                 prevhash: str,
                 minerAddr,
                 difficulty,
                 ):
        self.prevhash = prevhash
        self.number = None
        self.timestamp = None
        self.minerAddr = minerAddr
        self.difficulty = difficulty
        self.blockAddr = None

        self.__setTimestamp()

    @abstractmethod
    def __setBlockAddr(self):
        pass

    def __setTimestamp(self):
        self.timestamp = getCurrentTime()

    def setBlockSequenceInChain(self, sequence):
        self.number = sequence

    @property
    def hash(self):
        return encode_hex(keccak_256(str(self).encode('utf-8')))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.hash == other.hash

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return self.hash


class SemanticBlockHeader(BlockHeader):

    def __init__(self,
                 prevhash: str,

                 minerAddr,
                 difficulty,
                 committeeNodeAddr,

                 **kwargs
                 ):
        super().__init__(prevhash, minerAddr, difficulty)
        self.committeeNodeAddr = committeeNodeAddr
        self.locationsHistory = list[dict]

        self.__setBlockAddr()

        for key, value in kwargs.items():
            setattr(self, key, value)

    def __setBlockAddr(self):
        self.blockAddr = "Smblock-" + returnUniAddr()

    def logLocationsHistory(self, shardAddr):
        location = {shardAddr: self.number}
        self.locationsHistory.append(location)


class SemanticBlockBody:

    def __init__(self, semanticTXs: list[SemanticTransaction]):
        self.semanticTXs = semanticTXs

    def countTxs(self):
        return len(self.semanticTXs)


class SemanticBlock:

    def __init__(self, blockHeader: SemanticBlockHeader, blockBody: SemanticBlockBody):
        self.blockHeader = blockHeader
        self.blockBody = blockBody

    def getBlockTxsNums(self):
        return self.blockBody.countTxs();


class CommitteeBlockHeader(BlockHeader):

    def __init__(self,
                 prevhash: str,
                 minerAddr,
                 difficulty,
                 ):
        super().__init__(prevhash, minerAddr, difficulty)
        self.locationsHistory = list[dict]

        self.__setBlockAddr()

    def __setBlockAddr(self):
        self.blockAddr = "Cmblock-" + returnUniAddr()

    def logLocationsHistory(self, shardAddr):
        location = {shardAddr: self.number}
        self.locationsHistory.append(location)


class CommitteeBlockBody:

    def __init__(self, nodesActionsList):
        self.actionList = nodesActionsList


class CommitteeBlock:

    def __init__(self, blockHeader: SemanticBlockHeader, blockBody: CommitteeBlockBody,
                 shardingStrategy=None
                 ):
        self.blockHeader = blockHeader
        self.blockBody = blockBody

        self.shardingStrategy = shardingStrategy
        self.csMethodForSmShards = None

    def setShardingStrategy(self, shardingStrategy):
        self.shardingStrategy = shardingStrategy

    def setCsMethodForSmShards(self, csMethodForSmShards):
        self.csMethodForSmShards = csMethodForSmShards
