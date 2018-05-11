from socket import *
from threading import *


server_addr=gethostname()
server_port=9000

client_socket=socket(AF_INET,SOCK_STREAM)
client_socket.connect((server_addr,server_port))
message = client_socket.recv(1024)
print message

def send(connection_socket):
	while 1:
		destinationId = raw_input("userId for sending message :  ")
		message = raw_input("your message : ")
		connection_socket.send(destinationId)
		connection_socket.send(message)


def recv(connection_socket):
	while 1:
		message = connection_socket.recv(1024)
		print "\n",message

Thread(target=send, args=(client_socket,)).start()
Thread(target=recv, args=(client_socket,)).start()
