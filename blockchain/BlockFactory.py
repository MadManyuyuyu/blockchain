from blockchain.Transcation.SemanticTX import SemanticTransaction
from blockchain.Block.BCBlock import SemanticBlockBody, SemanticBlock, SemanticBlockHeader


class BlockFactory:
    def __init__(self):

        pass

    def getBlock(self, headerData: dict, txsData: list[dict]):

        scBlockHeader = self.__createBlockHeader(headerData)

        scBlockBody = self.__createBlockBody(txsData)

        return self.__createBlock(scBlockHeader, scBlockBody)

    def getBlockWithTxs(self, headerData, txs):

        scBlockHeader = self.__createBlockHeader(headerData)
        if type(txs) == list:
            scBlockBody = self.__createBlockBodyWithTxs(txs)
        else:
            scBlockBody = self.__createBlockBodyWithTxs([txs])

        return self.__createBlock(scBlockHeader, scBlockBody)

    def __createBlockHeader(self, headerData: dict):

        return SemanticBlockHeader(**headerData)

    def __createBlockBody(self, txsData: list[dict]):

        txsList = []
        for txdata in txsData:
            scTx = SemanticTransaction(**txdata)
            txsList.append(scTx)

        return SemanticBlockBody(txsList)

    def __createBlockBodyWithTxs(self, txs: list):

        return SemanticBlockBody(txs)

    def __createBlock(self, header: SemanticBlockHeader, body: SemanticBlockBody):

        return SemanticBlock(header, body)
