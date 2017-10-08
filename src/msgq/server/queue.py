from collections import OrderedDict

class TaskQueue(OrderedDict):

    def __init__(self, queue_name, max_size):
        self.queue_name = queue_name
        self.max_size = max_size
        super(TaskQueue, self).__init__()

    def enqueue(self, message, count):
        self[message] = count

    def dequeue(self, message):
        # The message will be popped from the main task queue only
        # when all consumers consumed this message.
        self[message] -= 1
        if self[message] == 0:
            self.pop(message)

    def is_full(self):
        if len(self) >= self.max_size:
            return True
        return False


class MessageQueue(OrderedDict):
    # A message will be requeued only for 3 times. After that message will
    # be discarded permanently
    ACK_LIMIT = 3

    def __init__(self, *args, **kwargs):
        self.ack_map = {}
        super(MessageQueue, self).__init__(*args, **kwargs)

    def is_empty(self):
        return len(self) == 0

    def enqueue(self, message, value=0):
        self[message] = value

    def requeue(self, message):
        self.queue.pop(message,None)
        if message not in self.ack_map:
            self.ack_map[message] = 3
        self.ack_map[message] -= 1
        if self.ack_map[message] > 0:
            self.enqueue(message)

    def delete(self, message):
        # All the messages that we're acknowledged will be removed.
        # This helps in checking which messages were not acknowledged at all.
        self.pop(message, None)
        self.ack_map.pop(message, None)

    def dequeue(self):
        '''
        Dequeue by default requeues the same message at the end.
        When the message is Acknowledged by consumer it'll be
        deleted from queue.
        Also when giving the message it'll give message only when all its
        parents already consumed it, and is free from all dependencies.
        '''
        msg = None
        if not self.is_empty():
            for i,v in self.items():
                if v == 0:
                    msg = i
                    self.requeue(msg)
                    break
        return msg