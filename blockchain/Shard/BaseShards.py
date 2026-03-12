from abc import abstractmethod

from utils.aboutTime import getCurrentTime


class CommonShard:

    def __init__(self, shardAddr):

        self.createdTime = None
        self.shardAddr = shardAddr
        self.blocksList = None
        self.blockFactory = None
        self.__setCreatedTime()

    def __setCreatedTime(self):

        self.createdTime = getCurrentTime()

    def addBlocks(self, blocks):

        if self.blocksList == None:
            self.blocksList = []
        if type(blocks) == list:
            for block in blocks:
                self.blocksList.append(block)
        else:
            self.blocksList.append(blocks)

        return len(self.blocksList)

    def setBlockFactory(self, blockFactory):

        self.blockFactory = blockFactory

    @abstractmethod
    def letUsRockNRoll(self, **arg):
        pass

    def __str__(self):
        return f" shardAddr = {self.shardAddr}, createdTime = {self.createdTime})"
