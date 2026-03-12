

import pickle

from Crypto.Hash import keccak

import binascii
import numpy as np
def getUniRawID(semanticData):
    array_str = np.array2string(semanticData, separator=',')
    return pickle.dumps(array_str)

def keccak_256(value):

    return keccak.new(digest_bits=256, data=value).digest()

def encode_hex(b):
    if isinstance(b, str):
        b = bytes(b, 'utf-8')
    if isinstance(b, (bytes, bytearray)):
        return str(binascii.hexlify(b), 'utf-8')
    raise TypeError('Value must be an instance of str or bytes')



