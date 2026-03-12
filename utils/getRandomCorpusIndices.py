
import random


def getCorpusIndiceDict(groupIndices,numOfIndices):

    all_elements = []
    for key in groupIndices:
        for value in groupIndices[key]:
            all_elements.append((key, value))


    sample_size = min(numOfIndices, len(all_elements))
    if sample_size == 0:
        return {}


    extracted_elements = random.sample(all_elements, sample_size)


    result = {key: [] for key in groupIndices}  # 为每个键初始化一个空列表


    for key, value in extracted_elements:
        result[key].append(value)


    result = {key: value for key, value in result.items() if value}



    for key in groupIndices:
        groupIndices[key] = [x for x in groupIndices[key] if (key, x) not in extracted_elements]


    return result

