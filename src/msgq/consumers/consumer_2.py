from msgq.client import BaseConsumer

class ConsumerTwo(BaseConsumer):
    queue = 'notifications'
    name = 'B'
    expression = 'User-'
    depends_on = ['A']

    def on_message(self, message):
        if message != 'null':
            print("Message received", message)
            super(ConsumerTwo, self).on_message(message)

c = ConsumerTwo('127.0.0.1', 9996)
c.start_consuming(poll_interval=2)