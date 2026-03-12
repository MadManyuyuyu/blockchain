class NodesDB:

    def __init__(self):

        self.bcNodesDB = {}

    def putNode(self, nodeUniId, nodeMemoryAddr):

        self.bcNodesDB[nodeUniId] = nodeMemoryAddr

        return True

    def putNodes(self, nodes: list):

        for node in nodes:
            self.bcNodesDB[node.nodeAddr] = node
        return len(self.bcNodesDB)

    def delNodeByUniId(self, nodeUniId):

        if nodeUniId in self.bcNodesDB:
            self.bcNodesDB.pop(nodeUniId)
            return True

        return False

    def returnNodeDBSize(self):

        return len(self.bcNodesDB)
