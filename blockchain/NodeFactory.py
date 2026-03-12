from blockchain.Node.SemanticNode import SemanticNode, RelayNode, CommitteeNode


class NodeFactory:
    def __init__(self):
        pass

    def createdCmNodesByLocation(self, location, nodeNum=1):

        if nodeNum > 1:
            cmNodes = []
            for _ in range(nodeNum):
                cmNode = CommitteeNode(nodeLocation=location)
                cmNodes.append(cmNode)
        else:
            cmNodes = CommitteeNode(nodeLocation=location)

        return cmNodes

    def createdSmNodesByLocation(self, location, nodeNum=1, hashRate=0):
        if nodeNum > 1:
            smNodes = []
            for _ in range(nodeNum):
                smNode = SemanticNode(nodeLocation=location, hashRate=hashRate)
                smNodes.append(smNode)
        else:
            smNodes = SemanticNode(nodeLocation=location, hashRate=hashRate)

        return smNodes

    def createdRlyNodesByLocation(self, location, nodeNum=1, controller=None):

        if nodeNum > 1:
            rlyNodes = []
            for _ in range(nodeNum):
                rlyNode = RelayNode(nodeLocation=location)
                rlyNode.controller = controller
                rlyNodes.append(rlyNode)
        else:
            rlyNodes = RelayNode(nodeLocation=location)
            rlyNodes.controller = controller
        return rlyNodes
