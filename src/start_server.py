import sys
import socketserver as SocketServer

from msgq.server import Server

HOST = "localhost"
PORT = 9999

if len(sys.argv) > 1:
    PORT = int(sys.argv[1])

server = SocketServer.UDPServer((HOST, PORT), Server)

print("Serving from localhost", PORT)

server.serve_forever()