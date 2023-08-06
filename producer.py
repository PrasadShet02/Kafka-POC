from flask import Flask,request,make_response,jsonify
import requests
import json
import socket
import sys
import time

my_port = None

def main():
    topicname = sys.argv[1]
    reply = {'message':'no'}
    port_broker = ''
    while reply['message']!='Yes':
        reply = json.loads(requests.get('http://localhost:9092/',headers={'Content-type':'Application/json'}).content.decode())
        port_broker = reply['port']
    while 1:
        message = input()
        reply = {'message':'no'}
        while reply['message'] != 'success':
            try:
                reply = json.loads(requests.post(f'http://localhost:{port_broker}/write',headers={'Content-type':'Application/json'},json={'producer':my_port,'topic':topicname,'message':message}).content.decode())
            except:
                while reply['message']!='Yes':
                    reply = json.loads(requests.get('http://localhost:9092/',headers={'Content-type':'Application/json'}).content.decode())
                    port_broker = reply['port']
if __name__ == '__main__':
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost', 0))
    port = sock.getsockname()[1]
    my_port = port 
    sock.close()
    main()