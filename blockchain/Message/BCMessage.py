from utils.aboutTime import getCurrentTime
from utils.getUniAddr import returnUniAddr


class BaseMessage:

    def __init__(self,
                 sender,
                 receiver

                 ):
        self.sender = sender
        self.receiver = receiver
        self.messageAddr = None
        self.createdTime = None
        self.__setCreatedTime()
        self.__setMessageAddr()

    def __setCreatedTime(self):
        self.createdTime = getCurrentTime()

    def __setMessageAddr(self):
        self.messageAddr = "mes-" + returnUniAddr()


class CmMessage(BaseMessage):
    def __init__(self,
                 sender,
                 receiver,
                 committeeContent
                 ):
        super().__init__(sender, receiver)
        self.committeeContent = committeeContent


class RelayMessage(BaseMessage):
    def __init__(self,
                 sender,
                 receiver,
                 relayContent
                 ):
        super().__init__(sender, receiver)
        self.relayContent = relayContent


class QueryMessage(BaseMessage):

    def __init__(self,
                 sender,
                 receiver,
                 queryContent
                 ):
        super().__init__(sender, receiver)
        self.queryContent = queryContent


class UploadMessage(BaseMessage):

    def __init__(self,
                 sender,
                 receiver,
                 uploadContent
                 ):
        super().__init__(sender, receiver)
        self.uploadContent = uploadContent


class DownloadMessage(BaseMessage):

    def __init__(self,
                 sender,
                 receiver,
                 downloadContent
                 ):
        super().__init__(sender, receiver)
        self.downloadContent = downloadContent
