import socket
import sys

SERVER = socket.gethostbyname(socket.gethostname())
FORMAT = 'utf-8'
Port= int(sys.argv[-1])
consumer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
consumer.connect ( (SERVER,Port))
topic=input("Enter topic name")
choice=input("Do you want data from begining or not")
if len(choice)>0:
    consumer.send (bytes("begin:"+topic, 'utf-8'))
    while True:
        response=consumer.recv(1024).decode()
        if len(response)>0:
            print(response)
else:
    consumer.send(f":{topic}".encode(FORMAT))
    while True:
        response=consumer.recv(1024).decode()
        if len(response)>0:
            print(response)
        if response!="Acknowledged":
            b=input("Would you wish to unsubscribe?")
            if b=="yes":
                consumer.send("$$:$$".encode(FORMAT))
                break


