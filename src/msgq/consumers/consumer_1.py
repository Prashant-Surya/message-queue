import sys

from msgq.client import consume_messages

host = '127.0.0.1'
port = 9996

if len(sys.argv) > 1:
    port = sys.argv[1]

def callback(message):
    print("Receved in callback for consumer 1", message)

kwargs = {
    'queue': 'notifications',
    'name': 'A',
    'expression': 'User-',
    'callback': callback
}

consume_messages(host, port, kwargs)