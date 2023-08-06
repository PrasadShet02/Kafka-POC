from flask import Flask,request,make_response,jsonify
import requests
import os
import shutil
import socket
import random
import threading
import time

leader = None
brokers = []
app = Flask(__name__)

def eligible_candidates():
    count = 0
    for i in brokers:
        if not is_port_in_use(i):
            count += 1
    return count == len(brokers)

def zookeeper():
    global leader
    while 1:
        print('next heartbeat')
        removal = []
        for i in brokers:
            if not is_port_in_use(i):
                print('Damage detected!Inspecting...')
                if i == leader:
                    print('The leader has died...initiating relection')
                    if len(brokers) == 1:
                        leader = None
                    else:
                        leader = elect_leader(i)
                removal.append(i)
                print('New leader is:',leader)
        for i in removal:
            brokers.remove(i)
        print('brokers:',brokers)
        if removal:
            for i in brokers:
                print(requests.post(f'http://localhost:{i}/update_leader',headers={'Content-type':'Application/json'},json={'leader':leader,'brokers':brokers}).content)
        time.sleep(10)

def elect_leader(old_leader):
    i = old_leader
    while i==old_leader:
        i = random.choice(brokers)
    return i

def is_port_in_use(port: int):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

@app.route('/',methods=['GET'])
def return_nearest_broker():
    global leader
    if leader is not None and is_port_in_use(leader):
        return jsonify(message='Yes',port=leader)
    else:
        return jsonify(message='Try after 10s',port=9092)

@app.route('/leader',methods=['POST'])
def get_leader():
    global brokers
    global leader
    print(request.get_json())
    if request.get_json()['port'] not in brokers:
        brokers.append(request.get_json()['port'])
    if leader is None or not is_port_in_use(leader):
        leader = request.get_json()['port']
    for i in brokers:
        if i==request.get_json()['port']:
            continue
        requests.post(f'http://localhost:{i}/update_broker',headers={'Content-type':'Application/json'},json={'brokers':brokers})
    return jsonify(leader=leader,brokers=brokers)

if __name__ == '__main__':
    zkpr = threading.Thread(target=zookeeper,args=tuple(),daemon=True)
    zkpr.start()
    app.run(host='localhost',port='9092')

