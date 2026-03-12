
from utils import encodePart


class BCTransaction:
    
    def __init__(self, sender, receiver, signature):

        self.sender = sender
        self.receiver = receiver
        self.signature = signature

    @property
    def hash(self):

        return encodePart.keccak_256(str(self).encode('utf-8'))

    def __repr__(self):

        return f'<{self.__class__.__name__}({self.hash})>'

    def __str__(self):

        return f'''<{self.__class__.__name__}(to:{self.receiver} sender:{self.sender}  signature:{self.signature})>'''

    def __eq__(self, other):

        return isinstance(other, self.__class__) and self.hash == other.hash
