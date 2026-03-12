
from collections import deque


class MyQ:
    def __init__(self):
        self._queue = deque()
        self._size = 0

    def addTxToQ(self, tx):

        self._queue.append(tx)
        self._size += 1

    def removeTxFromQ(self):

        if self.is_empty:
            raise IndexError("队列为空，无法执行出队操作")
        self._size -= 1
        return self._queue.popleft()

    @property
    def is_empty(self):

        return self._size == 0

    @property
    def size(self):

        return self._size

    def getFrontTx(self):

        if self.is_empty:
            raise IndexError("队列为空，无法查看队头元素")
        return self._queue[0]

    def __str__(self):

        return str(list(self._queue))
