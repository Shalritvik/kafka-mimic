import socket
import sys

SERVER = socket.gethostbyname(socket.gethostname())
FORMAT = 'utf-8'
Port= int(sys.argv[-1])
def fun():
    topic = input("Enter the topic:")
    producer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    producer.connect ((SERVER,Port))
    data = input("Enter topic data:")
    while True and data!="stop":
        producer.send (f'{topic}:{data}'.encode(FORMAT))
        receive=producer.recv (1024). decode()
        if len(receive)=="received":
            print(receive)
            break
    producer.close()
fun()

