from utils import encodePart, aboutTime, getUniAddr
from blockchain.Transcation.BCTransaction import BCTransaction


class SemanticTransaction(BCTransaction):
    def __init__(self, sender, receiver, signature, semanticData, **kwargs):

        super().__init__(sender, receiver, signature)
        self.semanticData = semanticData

        self.intoShardPoolTime = None

        self.confirmedTime = None

        self.sequenceNum = None

        self.txAddr = None
        self.createdTime = None
        self.__setCreatedTime()
        self.__setTxAddr()

        for key, value in kwargs.items():
            setattr(self, key, value)

    def __setTxAddr(self):

        self.txAddr = "smtx-" + getUniAddr.returnUniAddr()

    @property
    def hash(self):

        return encodePart.encode_hex(encodePart.keccak_256(str(self).encode('utf-8')))

    def __eq__(self, other):

        return isinstance(other, self.__class__) and self.hash == other.hash and self.semantic == other.semantic

    def setIntoShardPoolTime(self, shardAddr, intoPoolTime):

        self.intoShardPoolTime = {}
        self.intoShardPoolTime["shardAddr"] = shardAddr
        self.intoShardPoolTime["time"] = intoPoolTime

    def getConfirmedDelay(self):

        if self.confirmedTime == -1 or int(self.createdTime) > int(self.confirmedTime):
            raise ValueError(f"计算交易确认时延错误，确认时间为 {self.confirmedTime}, 交易创建时间为{self.createdTime}.")
        return int(self.confirmedTime) - int(self.createdTime)

    def setSequenceNum(self, sequenceNum):

        self.sequenceNum = sequenceNum

    def __setCreatedTime(self):
        self.createdTime = aboutTime.getCurrentTime()
