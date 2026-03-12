
import sys

from utils.encodePart import getUniRawID


class BCDownload:
    def __init__(self,bc,winSize=4):
        self.bc = bc 
        self.windSize = winSize 

    def getShardOfUploader(self,semanticData,smDataToNodeDict):

        smallData = getUniRawID(semanticData = semanticData)
        if smallData not in smDataToNodeDict:
            sys.exit()
        uploader = smDataToNodeDict[smallData]
        uploaderShardAddr = uploader.belongingChainAddr 
        
        return uploader, uploaderShardAddr






