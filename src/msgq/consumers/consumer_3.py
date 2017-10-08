import sys

from msgq.client import consume_messages

host = '127.0.0.1'
port = 9996

if len(sys.argv) > 1:
    port = sys.argv[1]

def callback(message):
    print("Recieved in callback for consumer 2", message)

kwargs = {
    'queue': 'notifications',
    'name': 'C',
    'expression': 'User-',
    'callback': callback,
    'depends_on': ['A', 'B']
}

consume_messages(host, port, kwargs)