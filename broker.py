from flask import Flask,request,make_response,jsonify
import requests
import socket
import random
import os
import shutil 
import sys
from collections import defaultdict
import json
import threading

leader = None
zookeeper = None
my_port = None
producers = defaultdict(lambda: list())
consumers = {}
brokers = []
app = Flask(__name__)

def is_port_in_use(port: int):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

@app.route('/save',methods=['POST','PUT'])
def save():
    global curr_offset
    data = request.get_json()
    path = f'./brokers/{my_port}/'
    path1 = ''
    if not os.path.isdir(path):
        sys.exit(0)
    for i in data:
        if i == 'Foldername':
            os.makedirs(path+data[i],exist_ok=True)
            path1 = path + data[i] + '/'
        if i == 'partition':
            #See if you can remove this
            if not os.path.isfile(path1+data['partition']):
                with open(path1+data['partition'],'w',encoding='utf-8') as f:
                    f.write('')
            with open(path1+data['partition'],'a',encoding='utf-8') as f:
                f.write(data['message']+'\n')
        if i == 'logfile':
            max_offset = 0
            with open(path+'logfile.txt','a',encoding='utf-8') as f:
                for line in data['logs'].split('\n'):
                    f.write(line)
                    w = line.split()
                    if w:
                        if w[0] == '0':
                            producers[w[1]].append(w[2])
                        else:
                            consumers[w[1]] = w[2]
    if my_port == leader:
        for i in brokers:
            if is_port_in_use(i) and i!=leader: #check if it returns string or int
                threading.Thread(target=save_data, args=(data['Foldername'],data['message'],data['partition'],data['logs']), daemon=True).start()
    return jsonify(message='write successful!',port=my_port)

@app.route('/read',methods=['POST'])
def read():
    print('read request received')
    data = request.get_json()
    offset = 0
    if not os.path.isdir(f'./brokers/{my_port}'):
        sys.exit(0)
    if data['consumer'] not in consumers:
        consumers[data['consumer']] = data['topic']
    topicname = data['topic']
    if not os.path.isdir(f'./brokers/{my_port}/{topicname}'):
        os.makedirs(f'./brokers/{my_port}/{topicname}',exist_ok=True)
        with open(f'./brokers/{my_port}/{topicname}/partition0.txt','w',encoding='utf-8') as f:
            f.write('')
            return jsonify(message='')
    if data['flag'] != 1:
        offset = 1
    logs = ''
    curr_offset = {}
    with open(f'./brokers/{my_port}/logfile.txt','r',encoding='utf-8') as f:
        if f.tell() == f.seek(0,2):
            return jsonify(message='')
        f.seek(0,0)
        lines = f.readlines()
        for line in lines:
            line = line.split()
            if line[2] == topicname:
                if line[3] not in curr_offset:
                    curr_offset[line[3]] = 0
                if line[3] in curr_offset and int(line[4])>curr_offset[line[3]]:
                    curr_offset[line[3]] = int(line[4]) if line[0] == '1' else curr_offset[line[3]]
    d = ''
    print(curr_offset)
    for i in os.listdir(f'./brokers/{my_port}/{topicname}'):
        with open(f'./brokers/{my_port}/{topicname}/{i}','r',encoding='utf-8') as f:
            if i in curr_offset:
                if offset:
                    f.seek(curr_offset[i],0)
                    d += f.readline()
                    curr_offset[i] = f.tell()
                else:
                    f.seek(0,0)
                    for word in f.readlines():
                        d += word[:-1] + '\n'
                    curr_offset[i] = f.tell()
                print('offset after read',curr_offset[i])
                logs += '1 '+str(data['consumer'])+' '+topicname+' '+i+' '+str(curr_offset[i])+'\n'
    d = d[:-1]
    with open(f'./brokers/{my_port}/logfile.txt','a',encoding='utf-8') as f:
        f.write(logs)
    print('read request processed')
    if my_port == leader:
        for i in brokers:
            if is_port_in_use(i) and i!=leader: #check if it returns string or int
                resp = requests.post(f'http://localhost:{i}/save',headers={'Content-tpye':'Application/json'},json={'logfile':1,'logs':logs}).content
    else:
        requests.post(f'http://localhost:{leader}/save',headers={'Content-tpye':'Application/json'},json={'logfile':1,'logs':logs})
    print('final output',d)
    return jsonify(message=d)

def save_data(topic,message,partition,logs):
    global brokers
    global leader
    for i in brokers:
        if i != leader:
            requests.post(f'http://localhost:{i}/save',headers={'Content-tpye':'Application/json'},json={'Foldername':topic,'partition':partition,'message':message,'logfile':1,'logs':logs})

@app.route('/write',methods=['POST'])
def write():
    data = request.get_json()
    print('write request received')
    if not os.path.isdir(f'./brokers/{my_port}'):
        sys.exit(0)
    if my_port != leader:
        return jsonify(message='No',port=leader)
    if data['producer'] not in producers or data['topic'] not in producers[data['producer']]:
        producers[data['producer']].append(data['topic'])
    topicname = data['topic']
    if not os.path.isdir(f'./brokers/{my_port}/{topicname}'):
        os.makedirs(f'./brokers/{my_port}/{topicname}',exist_ok=True)
        with open(f'./brokers/{my_port}/{topicname}/partition0.txt','w',encoding='utf-8') as f:
            f.write('')
    list_of_partitions = os.listdir(f'./brokers/{my_port}/{topicname}')
    print(list_of_partitions)
    file_pointers = [open(f'./brokers/{my_port}/{topicname}/{partition}','a') for partition in list_of_partitions]
    i = 0
    num_of_partitions = 0
    removal = []
    for i in file_pointers:
        val = i.seek(0,2)
        if val >= 500:
            removal.append(i)
        else:
            num_of_partitions += 1
    if removal:
        for i in removal:
            i.close()
            file_pointers.remove(i)
    partition_assigned = int(data['producer'])%len(list_of_partitions)
    fp = file_pointers[partition_assigned]
    fp.write(data['message']+'\n')
    logs = '0 '+str(data['producer'])+' '+topicname+' '+'partition'+str(partition_assigned)+'.txt'+' '+str(fp.tell())+'\n'
    with open(f'./brokers/{my_port}/logfile.txt','a') as f:
        f.write(logs)

    if fp.tell() >= 5:
        with open(f'./brokers/{my_port}/{topicname}/partition{len(list_of_partitions)}.txt','w',encoding='utf-8') as f:
            f.write('')
    for i in file_pointers:
        i.close()
    print('write completed')
    threading.Thread(target=save_data, args=(topicname,data['message'],'partition'+str(partition_assigned)+'.txt',logs), daemon=True).start()
    print('thread initiated!')
    return jsonify(message='success',port=leader)

@app.route('/update_leader',methods=['POST'])
def update_leader():
    global leader
    global brokers
    leader = request.get_json()['leader']
    brokers = request.get_json()['brokers']
    return jsonify(message='Hail the new leader!')

@app.route('/update_broker',methods=['POST'])
def update_brokers():
    global brokers
    brokers = request.get_json()['brokers']
    return jsonify(message='List updated')

@app.route('/connect',methods=['GET','post'])
def welcome_node():
    return jsonify(message='Welcome',path=f'./brokers/{leader}')

def talk_to_leader(port):
    global leader
    if port != leader:
        reply = requests.post(f'http://localhost:{leader}/connect',headers={'Content-type':'Application/json'},json={'port':port}).content
        reply = json.loads(reply.decode())
        if reply['message'] == 'Welcome':
            if os.path.isdir(f'./brokers/{port}'):
                shutil.rmtree(f'./brokers/{port}')
            # os.makedirs(f'./brokers/{port}')
            # with open(f'./brokers/{port}/logfile.txt','w',encoding='utf-8') as f:
            #     f.write(reply['logs'])
            shutil.copytree(reply['path'],f'./brokers/{port}')
    else:
        os.makedirs(f'./brokers/{leader}',exist_ok=True)
        with open(f'./brokers/{port}/logfile.txt','w',encoding='utf-8') as f:
            f.write('')

def connect_to_zookeeper(port):
    global zookeeper
    global leader
    global brokers
    zookeeper = 9092
    resp = requests.post(f'http://localhost:{zookeeper}/leader',headers={'Content-type':'Application/json'},json={'message':'Hey Zookeeper','port':port}).content
    resp = json.loads(resp.decode())
    print(resp,type(resp))
    leader = resp['leader']
    brokers = resp['brokers']

if __name__ == '__main__':
    port_no = random.randint(7000,9000)
    while is_port_in_use(port_no):
        port_no = random.randint(7000,9000)
    while leader is None:
        connect_to_zookeeper(port_no)
    talk_to_leader(port_no)
    my_port = port_no
    app.run(host='localhost',port=port_no)
