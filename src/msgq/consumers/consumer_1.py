import sys

from msgq.client import BaseConsumer

class ConsumerOne(BaseConsumer):
    queue = 'notifications'
    name = 'A'
    expression = 'User-'

    def on_message(self, message):
        if message != "null":
            print("Message received", message)
            super(ConsumerOne, self).on_message(message)

c = ConsumerOne('127.0.0.1', 9996)
try:
    c.start_consuming(poll_interval=2)
except KeyboardInterrupt:
    c.unsubscribe()