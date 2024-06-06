import queue
from collections import deque

class Hops:
    def __init__(self, transaction_tag, is_last, operations):
        self.transaction_tag = transaction_tag
        self.is_last = is_last
        self.operations = operations  # List of operations

    def __repr__(self):
        return (f"Hops(transaction_tag={self.transaction_tag}, is_last={self.is_last}, "
                f"operations={self.operations})")

class HopsQueue:
    def __init__(self):
        self.queue = deque()

    def enqueue(self, hop):
        self.queue.append(hop)

    def dequeue(self):
        if self.is_empty():
            raise IndexError("dequeue from an empty queue")
        return self.queue.popleft()

    def is_empty(self):
        return len(self.queue) == 0

    def peek(self):
        if self.is_empty():
            raise IndexError("peek from an empty queue")
        return self.queue[0]

    def __len__(self):
        return len(self.queue)

    def __repr__(self):
        return f"HopsQueue({list(self.queue)})"

    def remove_all(self, transaction_tag):
        self.queue = deque([hop for hop in self.queue if hop.transaction_tag != transaction_tag])

class DoneList:
    def __init__(self):
        self.done_list = []

    def add(self, transaction_tag):
        if transaction_tag not in self.done_list:
            self.done_list.append(transaction_tag)

    def contains(self, transaction_tag):
        return transaction_tag in self.done_list

    def __repr__(self):
        return f"DoneList({self.done_list})"

class Node: 
    threadID = None
    def __init__(self, nodeNumber):
        self.nodeNumber = nodeNumber
        self.csvFile = str(nodeNumber) + ".csv"
        self.message_queue = globalMessageQueue[nodeNumber]
        self.response_queue = globalResponseQueue[nodeNumber]

class Request:
    def __init__(self, hop):
        self.hop = hop

class Response: 
    def __init__(self, status, body):
        self.status = status
        self.body = body

class Queue:
    threadID = None
    def __init__(self, queueNumber) -> None:
        self.queue = globalQueuesList[queueNumber]
        self.queueNumber = queueNumber
        self.message_queue = globalMessageQueue[queueNumber]
        self.response_queue = globalResponseQueue[queueNumber]

numberOfPartitions = 4

globalMessageQueue = [queue.Queue() for _ in range(numberOfPartitions)]
globalResponseQueue = [queue.Queue() for _ in range(numberOfPartitions)]
globalQueuesList = [HopsQueue() for _ in range(numberOfPartitions)]
allQueues = [Queue(i) for i in range(numberOfPartitions)]
allNodes = [Node(i) for i in range(numberOfPartitions)]