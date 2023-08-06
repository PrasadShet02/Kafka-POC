from flask import Flask, request, jsonify
import requests
import json
import socket
import os
import sys
import time

app = Flask(__name__)

my_port = None

def main():
    topicname = sys.argv[1]
    reply = {'message':'no'}
    port_broker = ''
    flag_passed = 0
    while reply['message']!='Yes':
        reply = json.loads(requests.get('http://localhost:9092/',headers={'Content-type':'Application/json'}).content.decode())
        port_broker = reply['port']
    while 1:
        reply = {'message':'no'}
        flag = 0
        if not flag_passed and len(sys.argv) > 2 and sys.argv[2] == '--from-beginning':
            flag = 1
            flag_passed = 1
        while reply['message'] == 'no':
            try:
                reply = requests.post(f'http://localhost:{port_broker}/read',headers={'Content-type':'Application/json'},json={'consumer':my_port,'topic':topicname, 'flag':flag}).content
                reply = json.loads(reply.decode())
            except:
                while reply['message']!='Yes':
                    reply = json.loads(requests.get('http://localhost:9092/',headers={'Content-type':'Application/json'}).content.decode())
                    port_broker = reply['port']
                reply['message'] = ''
        if reply['message'] != '':
            print(reply['message'])
        else:
            time.sleep(5)
if __name__ == '__main__':
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost', 0))
    port = sock.getsockname()[1]
    my_port = port 
    sock.close()
    main()
