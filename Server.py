#milad teimouri 95725127

from socket import *
from threading import *

index = 1
server_port=9000
server_socket = socket(AF_INET,SOCK_STREAM)
server_socket.bind(('',server_port))
server_socket.listen(20) # max number of connected client
clients_socket = {}

def client(connection_socket,address,index):
	while 1:
		try:
			destinationId = connection_socket.recv(10)
			message = connection_socket.recv(1024)
			if(clients_socket.has_key(destinationId)):
				destinatin_socket = clients_socket[destinationId]
				destinatin_socket.send("# user "+index+" => "+message)
				messageToCep = index+"->"+destinationId+"#"+message 
				cep_socket.send(messageToCep+'\n')
				print messageToCep
			else:
				connection_socket.send("the userId you write now doesn't exist in the chat :(")
		except Exception, e:
			del clients_socket[destinationId]
			print "user ",index,"removed from chat"
			break
		
cep_socket , cep_address = server_socket.accept()
cep_socket.settimeout(None)
print "Flink Event Process joined"

while True:
	connection_socket , address = server_socket.accept()
	connection_socket.settimeout(None)
	clients_socket[str(index)] = connection_socket
	print "user with Id",index , "joined chat"
	connection_socket.send("Welcome to chat, your Id="+str(index))
	Thread(target=client, args=(connection_socket,address,str(index),)).start()
	index = index +1

