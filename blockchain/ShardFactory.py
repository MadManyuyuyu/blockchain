from blockchain.Shard.Shards import CommitteeShard
from blockchain.Shard.Shards import SemanticShard
from utils.getUniAddr import returnUniAddr


class ShardFactory:
    def __init__(self):
        self.isCmShardExisted = False

    def createCommitteeShard(self, cmVerifier):

        if self.isCmShardExisted is True:
            return False
        else:
            self.isCmShardExisted = True
            return CommitteeShard(cmVerifier)

    @staticmethod
    def createSemanticShards(shardsNum=1):

        if shardsNum > 1:
            smShards = []
            for _ in range(shardsNum):
                smShards.append(SemanticShard())
        else:
            smShards = SemanticShard()

        return smShards
