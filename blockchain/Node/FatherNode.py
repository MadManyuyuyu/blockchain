from abc import abstractmethod
from utils.aboutTime import getCurrentTime


class BaseNode:

    def __init__(self, nodeLocation):
        self.belongingChainAddr = None
        self.nodeAddr = None
        self.nodeLocation = nodeLocation
        self.character = "honest"
        self.queriedHistoryList = None
        self.nodeCreatedTime = None

        self.__setNodeCreatedTime()

    @abstractmethod
    def __setNodeAddr(self):
        pass

    def setNodeBelongingChainAddr(self, shardAddr: str):
        self.belongingChainAddr = shardAddr
        return self.belongingChainAddr

    def setNodeCharacter(self, newCharacter):
        self.character = newCharacter
        return newCharacter

    def __setNodeCreatedTime(self):
        self.nodeCreatedTime = getCurrentTime()

    @abstractmethod
    def queryTxs(self, queryMessage):
        pass

    @abstractmethod
    def __addQueriesToHistory(self, queries):
        pass

    def __str__(self):
        return f"addr = {self.nodeAddr[:6]}, location ={self.nodeLocation},createdTime={self.nodeCreatedTime}, belongshard ={self.belongingChainAddr}"
