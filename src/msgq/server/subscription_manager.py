from msgq.server.queue import MessageQueue, TaskQueue

class Subscriber(object):

    def __init__(self, expression, name, queue_name, **kwargs):
        self._expression = expression
        self._name = name
        self._queue = queue_name
        self._parents = []
        self._dependencies = []
        self.message_queue = MessageQueue()

    @property
    def expression(self):
        return self._expression

    @property
    def name(self):
        return self._name

    @property
    def queue(self):
        return self._queue

    @property
    def parents_count(self):
        return len(self._parents)

    def add_parent(self, subscriber):
        self._parents.append(subscriber)

    def add_dependency(self, subscriber):
        self._dependencies.append(subscriber)

    def is_independent(self):
        if len(self._parents) > 0:
            return False
        return True

    def ack_message(self, message):
        self.message_queue.delete(message)
        for dependency in self._dependencies:
            dependency.message_queue[message] -= 1

    def consume(self):
        msg = self.message_queue.dequeue()
        return msg


class QueueManager(object):

    def __init__(self, queue_name, max_size=5):
        self.queue_name = queue_name
        self._subscribers = {}
        self.queue = TaskQueue(queue_name, max_size)

    def subscribe(self, subscriber):
        expression = subscriber.expression
        if expression not in self._subscribers:
            self._subscribers[expression] = {}
        self._subscribers[expression][subscriber.name] = subscriber

    def unsubscribe(self, subscriber):
        self._subscribers[subscriber.expression].pop(subscriber.name)

    def get_subscriber_by_name(self, name, expression):
        sub = None
        if (expression in self._subscribers and 
                name in self._subscribers[expression]):
            sub = self._subscribers[expression][name]
        return sub

    def push_messages(self, message):
        count = 0
        if self.queue.is_full():
            return False
        for expresssion in self._subscribers:
            if expresssion in message:
                for _name, sub in self._subscribers[expresssion].items():
                    sub.message_queue.enqueue(message, sub.parents_count)
                    count += 1
        if count > 0:
            self.queue.enqueue(message, count)
        return True

    def subscription_queues_info(self):
        for expression in self._subscribers:
            for subscriber in self._subscribers[expression].values():
                print("Subscriber ", subscriber.name)
                print(subscriber.message_queue)

class SubscriptionHandler(object):
    _queues = {}

    @classmethod
    def get_manager(cls, queue_name):
        manager = None
        if queue_name in cls._queues:
            manager = cls._queues[queue_name]
        else:
            manager = QueueManager(queue_name)
            cls._queues[queue_name] = manager
        return manager