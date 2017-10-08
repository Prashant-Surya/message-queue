#!/usr/bin/env python3
import json
import socketserver as SocketServer

from msgq.server.subscription_manager import Subscriber, SubscriptionHandler

class Server(SocketServer.BaseRequestHandler):

    @staticmethod
    def decode(message):
        return message.decode('utf-8')

    def action_method_map(self, action):
        ACTION_MAP = {
            'PUBLISH': self.publish,
            'SUBSCRIBE': self.subscribe,
            'GET': self.consume,
            'ACK': self.acknowledge
        }
        return ACTION_MAP.get(action, None)

    def handle(self):
        json_payload = self.request[0].strip()
        json_payload = self.decode(json_payload)
        self.socket = self.request[1]
        #print("Recevied in server ", json_payload)
        payload = json.loads(json_payload)
        action = payload.pop('action')
        self.queue_manager = SubscriptionHandler.get_manager(
            payload['queue_name'])
        method = self.action_method_map(action)
        if not method:
            print("Invalid action ", action)
            return
        method(payload)
        '''
        if action == 'PUBLISH':
            self.publish(payload)
        elif action == 'SUBSCRIBE':
            self.subscribe(payload)
        elif action == 'GET':
            self.consume(payload)
        elif action == 'ACK':
            self.acknowledge(payload)
        '''

    def consume(self, payload):
        subscriber = self.queue_manager.get_subscriber_by_name(
            payload['name'], payload['expression'])
        message = subscriber.consume()
        if not message:
            message = "null"
        self.reply(message)

    def publish(self, payload):
        message = payload.get('data')
        queue_manager = SubscriptionHandler.get_manager(
            payload['queue_name'])
        is_pushed = queue_manager.push_messages(message)
        if not is_pushed:
            print("Queue Full")

    def acknowledge(self, payload):
        message = payload.get('data')
        subscriber_name = payload.get('name')
        expression = payload.get('expression')
        queue_manager = SubscriptionHandler.get_manager(
            payload['queue_name'])
        subscriber = queue_manager.get_subscriber_by_name(subscriber_name,
                 expression)
        subscriber.ack_message(message)
        task_queue = queue_manager.queue
        task_queue.dequeue(message)

    def subscribe(self, payload):
        queue_manager = SubscriptionHandler.get_manager(
            payload['queue_name'])
        subscriber = Subscriber(**payload)
        queue_manager.subscribe(subscriber)

        # Adding current subscriber as dependency to the parent
        # dependencies.
        for dependency in payload['depends_on']:
            sub = queue_manager.get_subscriber_by_name(dependency, 
                payload['expression'])
            if sub:
                sub.add_dependency(dependency)
                subscriber.add_parent(sub)

        self.reply('Subscribed {0} to topic {1} of queue {2}'.format(
                            subscriber.name, payload['expression'],
                            subscriber.queue))

    def reply(self, message):
        self.socket.sendto(message.encode('utf-8'), self.client_address)

if __name__ == "__main__":
    HOST, PORT = "localhost", 9996

    # Create the server, binding to localhost on port 9996
    server = SocketServer.UDPServer((HOST, PORT), Server)
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    print("Serving from localhost 9996")
    server.serve_forever()