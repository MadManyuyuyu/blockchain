def newLoadDataForGC(rawPath, clusterPath=None, numOfRow=200000):

    fullSizeData = np.load(rawPath)
    embeddings = fullSizeData[:numOfRow]


    unique_rows, first_indices, inverse_indices, counts = np.unique(
        embeddings, axis=0, return_index=True, return_inverse=True, return_counts=True
    )


    duplicate_indices = np.where(counts > 1)[0]

    all_duplicate_indices = [np.where(inverse_indices == idx)[0][1:].tolist() for idx in duplicate_indices]


    flattened_indices = [idx for sublist in all_duplicate_indices for idx in sublist]
    flattened_indices = set(flattened_indices)

    with open(clusterPath, 'r', encoding='utf-8') as file:
        groupIndices = json.load(file)

    for key, value in groupIndices.items():
        groupIndices[key] = [int(v) for v in value]

    for key in groupIndices:
        groupIndices[key] = [x for x in groupIndices[key] if x < numOfRow]
        if len(groupIndices[key]) == 0:
            groupIndices.pop(key)
    groupIndices = {key: [x for x in value if x not in flattened_indices] for key, value in groupIndices.items()}

    numOfGroup = len(groupIndices)
    return embeddings, groupIndices, numOfGroup




if __name__ == "__main__":

    with open(None, 'r', encoding='utf-8') as file:
        hyperParms = json.load(file)

    embeddings, groupIndices, numOfGroup = newLoadDataForGC(rawPath=hyperParms["inputPath"],clusterPath=hyperParms["clusterPath"],numOfRow=None)
    numOfInitialCorpus = None
    hyperParms["initCorpus"] = embeddings[:numOfInitialCorpus]
    hyperParms["runningCorpus"] = embeddings[numOfInitialCorpus:]
    hyperParms["queryCorpus"] = embeddings

    bc = SmBC(hyperParms=hyperParms, corpus=embeddings, groupIndices=groupIndices, numOfGroup=numOfGroup,
              numOfInitialCorpus=numOfInitialCorpus)
    quer = Queries(bc=bc)

    bc.initBC()

    bc.ET930AM()

    quer.testWithOldCorpus(testCorpus=embeddings,checkResults=None,shardTopResult=None)
