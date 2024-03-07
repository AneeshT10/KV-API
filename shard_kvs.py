import os
import requests
from flask import Flask, request
from helper_functions import *
import threading

app = Flask(__name__)

sa = os.environ.get('SOCKET_ADDRESS', '')
vw = os.environ.get('VIEW', '') # preconfigured?
shard_count = os.environ.get('SHARD_COUNT', '')

# Create initial view list 
view_list = list(vw.split(","))
kvlist = {}
#Vector clock data
vc_map = {}
message_queue_a = []
message_queue_d = []
shard_list = {}

#broadcasting view to all other replicas
def put_view(main_url, sa):
    try:
        response = requests.put(main_url, json = {"socket-address": sa},timeout=1)
    except:
        pass
if not shard_count:
    for addy in view_list:
        if addy != sa:
            main_url = f'http://{addy}/view'
            thread = threading.Thread(target=put_view, args=(main_url, sa))
            thread.start()
#----------------------------------------
print("PASS")
for addy in view_list:
    try:
        s_url = f'http://{addy}/shard_list'
        sl = requests.get(s_url, timeout = 1).json()["shard_list"]
        shard_list = sl
        break
    except:
        continue
if shard_count: # represent nodes during startup
    #get shard_list from another replica
    if not shard_list: #Initially, if shard_list is empty
        for i in range(int(shard_count)):
            string_id = 's' + str(i+1)
            shard_list[string_id] = []
        shard_list["s1"].append(sa)
    else:
        insert_shard_key = optimal_insert(shard_list)
        for addy in view_list:
            try:
                data = {"isk":insert_shard_key, "sender":sa}
                s_url = f'http://{addy}/shard_list'
                requests.put(s_url, json = data, timeout = 1)
            except:
                continue

    


@app.route("/shard_list", methods = ['GET','PUT'])
def sh_list():
    if request.method == 'GET':
        return {"shard_list": shard_list}, 200
    else:
        data = request.get_json(force = True)
        shard_list[data["isk"]].append(data["sender"])
        return {"message": "done"}, 200
        
@app.route("/view", methods = ['GET', 'PUT', 'DELETE'])
def view():
    if request.method == 'GET':
        return {"view": view_list, "shard_list" : shard_list}, 200
    try:
        data = request.get_json(force = True)
    except:
        return {"error": "PUT request does not provide socket address"}, 400 
    if request.method == 'PUT':
        if data["socket-address"] not in vc_map.keys():
            vc_map[data["socket-address"]] = 0
        if data["socket-address"] not in view_list:
            view_list.append(data["socket-address"])
            return {"result": "added"}, 201
        else:
            return {"result": "already present"}, 200
    else :
        if data["socket-address"] in view_list:
            view_list.remove(data["socket-address"])
            return {"result": "deleted"}, 200
        else:
            return {"error": "View has no such replica"}, 404


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8090, debug=True)