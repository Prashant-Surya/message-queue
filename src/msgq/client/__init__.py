import json, time, socket


class SocketClient(object):

    def __init__(self, HOST, PORT):
        self.HOST = HOST
        self.PORT = PORT
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def decode(self, message):
        message = message.decode('utf-8')
        return message

    def connect(self):
        self.sock.connect((self.HOST, self.PORT))
        print("Connection established to {0}:{1}".format(
            self.HOST, self.PORT))

    def send(self, message):
        message = json.dumps(message)
        message = message.encode('utf-8')
        self.sock.sendto(message, (self.HOST, self.PORT))

    def finish(self):
        self.sock.close()


class Producer(SocketClient):
    def __init__(self, *args, **kwargs):
        super(Producer, self).__init__(*args, **kwargs)
        self.connect()

    def publish(self, queue_name, message):
        message = {
            'action': 'PUBLISH',
            'queue_name': queue_name,
            'data': message
        }
        self.send(message)


class BaseConsumer(SocketClient):
    queue = ''
    expression = ''
    name = ''
    depends_on = []

    def subscribe(self):
        self.connect()
        message = {
            'action': 'SUBSCRIBE',
            'queue_name': self.queue,
            'name': self.name,
            'expression': self.expression,
            'depends_on': self.depends_on
        }
        self.send(message)
        received_data = self.decode(self.sock.recv(10240))
        print(received_data)

    def get_message(self):
        message = {
            'action': 'GET',
            'name': self.name,
            'queue_name': self.queue,
            'expression': self.expression
        }
        self.send(message)

    def ack_message(self, msg):
        message = {
            'action': 'ACK',
            'name': self.name,
            'queue_name': self.queue,
            'expression': self.expression,
            'data': msg
        }
        self.send(message)

    def on_message(self, message):
        if message != 'null':
            self.ack_message(message)

    def start_consuming(self, poll_interval=0.5):
        self.subscribe()
        time.sleep(poll_interval)
        while True:
            #self.connect()
            self.get_message()
            received_data = self.decode(self.sock.recv(1024000))
            if received_data:
                self.on_message(received_data)
            time.sleep(poll_interval)