import os
import requests
from flask import Flask, request
from helper_functions import *
from collections import defaultdict
import threading
import hashlib

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

kvaddlock = threading.Lock()
put_lock = threading.Lock()


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
for addy in view_list:
    if addy != sa:
        try:
            s_url = f'http://{addy}/shard_list'
            print("sending get on", s_url)
            shard_list = requests.get(s_url, timeout = 1).json()["shard_list"]
            break
        except:
            continue
if shard_count: # represent nodes during startup
    #get shard_list from another replica
     #start up vc_map
    for addy in view_list:
        vc_map[addy] = 0
    for i in range(int(shard_count)):
            string_id = 's' + str(i+1)
            shard_list[string_id] = []
    for addy in view_list:
        insert_shard_key = optimal_insert(shard_list)
        shard_list[insert_shard_key].append(addy)

    
@app.route("/shard/ids", methods = ['GET'])
def ret_shardid():
    return {"shard-ids": list(shard_list.keys())}, 200

@app.route("/shard/node-shard-id", methods = ['GET'])
def ret_node_shard_id():
    for shard_id in shard_list:
        if sa in shard_list[shard_id]:
            return {"node-shard-id": shard_id}, 200
    return {"error" : "not part of any shard"}, 404

@app.route("/shard/members/<ID>", methods = ['GET'])
def ret_member_id(ID):
    if ID in shard_list.keys():
        return {"shard-members": shard_list[ID]}, 200
    else:
        return {"error" : "shard id does not exist"}, 404

@app.route("/shard/key-count/<ID>", methods = ['GET'])
def num_keys(ID):
    url = f'http://{sa}/shard/node-shard-id'
    current_shard = requests.get(url, timeout=1).json()["node-shard-id"]
    if ID == current_shard:
        return {"shard-key-count": len(kvlist)}
    else:
        for addy in shard_list[ID]:
            try:
                forward_url = f'http://{addy}/shard/key-count/{ID}'
                forward_count = requests.get(forward_url, timeout=1).json()["shard-key-count"]
                return {"shard-key-count": forward_count}
            except:
                continue
    
@app.route("/shard/add-member/<ID>", methods = ['PUT'])
def add_node(ID):
    try:
        data = request.get_json(force = True)
        if (data["socket-address"] not in view_list) or (ID not in shard_list.keys()):
            return {"error" : "ID or IP does not exists"}, 404
        shard_list[ID].append(data["socket-address"])
        add_to_shard_url = f'http://{data["socket-address"]}/kvlist_add/{ID}'
        r = requests.put(add_to_shard_url, timeout=1)
        #broadcast new node in shards
        d1 = {"isk":ID, "sender": data["socket-address"]}
        for addy in view_list:
            if addy != sa:
                try:
                    s_url = f'http://{addy}/shard_list'
                    requests.put(s_url, json = d1, timeout = 1)
                except:
                    continue
        return {"result": "node added to shard"}, 200
    except:
        return {"error": "some other error"}, 400
    
@app.route("/shard/reshard", methods = ['PUT'])
def reshard():
    #delete non functional nodes before attempting to reshard
    current_node_del_list = []
    for node in view_list:
        del_url = f'http://{node}/view'
        try:
            res = requests.get(del_url, timeout=1)
        except:
            current_node_del_list.append(node)
            for ip in view_list:
                if ip == sa:
                    continue
                try:
                    r = requests.delete(del_url, timeout=1)
                except:
                    continue
    for ad in current_node_del_list:
        try:
            view_list.remove(ad)
        except:
            continue


    try:
        new_shard_list = {}
        new_kv_list = {}
        data = request.get_json(force = True)
        reshard_count = data["shard-count"]
        print(reshard_count)
        if len(view_list)/reshard_count < 2: # too little nodes to support new shard count
            return {"error": "Not enough nodes to provide fault tolerance with requested shard count"}, 400
        #otherwise we reshard
        #Naive solution
        for i in range(int(reshard_count)):
                string_id = 's' + str(i+1)
                new_shard_list[string_id] = []
                new_kv_list[string_id] = {}
        for addy in view_list:
            insert_shard_key = optimal_insert(new_shard_list)
            new_shard_list[insert_shard_key].append(addy)
            kvlist_url = f'http://{addy}/kvlist'
            try:
                kvst = requests.get(kvlist_url, timeout=1).json()["kvlist"]
            except:
                continue
            for key in kvst: # list of keys for each shard is assigned
                key_hash = (int(hashlib.md5(key.encode()).hexdigest(), 16)% int(reshard_count))+1
                key_shard_assign = 's' + str(key_hash)
                (new_kv_list[key_shard_assign])[key] = kvst[key]
        #private API to set all nodes
        # write newkvlist
        '''with open('debug.txt', 'w+') as f:
            f.write("newkvlist" + str(new_kv_list))
        f.close()'''
        for current_shard in new_shard_list:
            for ip in new_shard_list[current_shard]:
                url = f'http://{ip}/updateshards'
                send_data = {"shard_list" : new_shard_list, "kvlist" : new_kv_list[current_shard], "shard_count" : reshard_count} 
                try:
                    resp = requests.put(url, json = send_data, timeout=1)
                except:
                    continue
        return {"result": "resharded"}, 200
    except:
        return {"error" : "reshard not working properly"}, 503
        

#API to set resharded shardlist and kvlist
@app.route("/updateshards", methods = ['PUT'])
def updateshards():
    global kvlist
    global shard_list
    global shard_count

    data = request.get_json(force = True)
    kvlist = data["kvlist"]
    shard_list = data["shard_list"]
    shard_count = data["shard_count"]
    return {"result" : "done"}, 200

    


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
        return {"view": view_list}, 200 #remember to reset view!!!
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
            #del vc_map[data["socket-address"]]
            return {"result": "deleted"}, 200
        else:
            return {"error": "View has no such replica"}, 404
        
@app.route("/kvlist", methods = ['GET'])
def kvst():
    return {"kvlist": kvlist}

@app.route("/vcmap", methods = ['GET'])
def vcmap():
    return {"vcmap": vc_map}

@app.route("/kvlist_add/<ID>", methods = ['PUT'])
def kvlist_update(ID):
    global kvlist
    global vc_map
    for addy in shard_list[ID]:
        try:
            url = f'http://{addy}/kvlist'
            vcmap_url = f'http://{addy}/vcmap'
            vc = requests.get(vcmap_url, timeout = 1).json()["vcmap"]
            kvl = requests.get(url, timeout = 1).json()["kvlist"]
            kvlist = kvl
            vc_map = vc
            break
        except:
            continue
    return {"result" : "kvstore updated"}, 200

#OLD CODE
#From main replica to receiving replica
@app.route("/kvmod/<key>", methods = ['PUT', 'DELETE'])
def kvadd(key):
    kvaddlock.acquire()
    global vc_map
    if request.method == 'PUT':
        data = request.get_json(force = True)
        val = data["value"]
        sender = data["sender"]
        meta_data = data["causal-metadata"]
        key_hash = data["key_hash"]

        c_url = f'http://{sa}/shard/node-shard-id'
        current_shard = requests.get(c_url, timeout=1).json()["node-shard-id"]

        if not deliverable(meta_data, vc_map, sender): #incoming is too far ahead
            #add it to a queue
            message_queue_a.append({"kvpair":(key,val), "causal-metadata" : meta_data, "sender": sender, "key_hash": key_hash})
            kvaddlock.release()
            return {"result": "done"}, 201
        else: #msg is deliverable
            vc_map = vc_combine(meta_data, vc_map)
            if current_shard == key_hash:#adds key to right shard
                kvlist[key] =  val

            d_count = 1
            while(d_count > 0):
                del_msg = []
                d_count = 0
                ini = 0
                for msg in message_queue_a:
                    if deliverable(msg["causal-metadata"], vc_map, msg["sender"]): # it is deliverable
                        del_msg.append(msg)
                        if current_shard == msg["key_hash"]:#adds key to right shard
                            kvlist[msg["kvpair"](0)] = msg["kvpair"](1)
                        d_count += 1
                        vc_map = vc_combine(msg["causal-metadata"], vc_map)
                for msg in del_msg:
                    message_queue_a.remove(msg)
            kvaddlock.release()
            return {"result": "done"}, 201
    else:
        data = request.get_json(force = True)
        sender = data["sender"]
        meta_data = data["causal-metadata"]
        if not deliverable(meta_data, vc_map, sender): #incoming is too far ahead
            #add it to a queue
            message_queue_d.append({"key":key, "causal-metadata" : meta_data, "sender": sender, "key_hash": key_hash})
            kvaddlock.release()
            return {"result": "done"}, 201
        else: #msg is deliverable
            vc_map = vc_combine(meta_data, vc_map)
            if current_shard == key_hash:#deletes key at right shard
                del kvlist[key]

            d_count = 1
            while(d_count > 0):
                del_msg = []
                d_count = 0
                ini = 0
                for msg in message_queue_d:
                    if deliverable(msg["causal-metadata"], vc_map, msg["sender"]): # it is deliverable
                        del_msg.append(msg)
                        if current_shard == msg["key_hash"]:#deletes key at right shard
                            del kvlist[msg["key"]]
                        d_count += 1
                        vc_map = vc_combine(msg["causal-metadata"], vc_map)
                for msg in del_msg:
                    message_queue_d.remove(msg)
            kvaddlock.release()
            return {"result": "done"}, 201

#Key value operations
@app.route('/kvs', strict_slashes=False, defaults={'key': ''}, methods=['GET', 'PUT', 'DELETE'])
@app.route("/kvs/<key>", methods = ['GET', 'PUT', 'DELETE'])
def kvs(key):
    if request.method != 'GET':
        put_lock.acquire()
    if request.method == 'PUT':
        try:
            data = request.get_json(force = True)
            key_hash = (int(hashlib.md5(key.encode()).hexdigest(), 16)% int(shard_count))+1
            key_shard_assign = 's' + str(key_hash) # represents the hashed shard id

            c_url = f'http://{sa}/shard/node-shard-id'
            current_shard = requests.get(c_url, timeout=1).json()["node-shard-id"]
            #If the node is not part of specified hashed shard from input key
            if current_shard != key_shard_assign:
                for ip in shard_list[key_shard_assign]:
                    try:
                        forward_url = f'http://{ip}/kvs/{key}'
                        response = requests.put(forward_url, json = data, timeout=1)
                        put_lock.release()
                        return response.json(), response.status_code
                    except:
                        continue
                put_lock.release()
                return {"error" : "no replica to forward to"}, 500

            meta_data = data["causal-metadata"]
            val = data["value"]
            if 'value' not in data.keys():
                put_lock.release()
                return {"error": "PUT request does not specify a value"}, 400
            elif len(key) > 50:
                put_lock.release()
                return {"error": "Key is too long"}, 400
            if meta_data is not None:
                if not check_equal(meta_data, vc_map):
                    put_lock.release()
                    return {"error": "Causal dependencies not satisfied; try again later"}, 503
            vc_map[sa] += 1 #Message about to be sent
            meta_data = vc_map #Updating metadata to match current main replica vector clock
            #broadcast replace to every other replica
            ad_rm = []
            vc_map_holder = vc_map.copy()
            view_list_holder = view_list.copy()
            def put_kvs_broadcast():
                for addy in view_list_holder:
                    if addy == sa:
                        continue
                    try:
                        broadcast_url = f'http://{addy}/kvmod/{key}'
                        requests.put(broadcast_url, json = {"value" : val, "causal-metadata" : vc_map_holder, "sender": sa, "key_hash" : key_shard_assign}, timeout=1)
                    except: # if replica is not repsonding delete from all views
                        ad_rm.append(addy)
                        for adr in view_list_holder:
                            if adr != addy and adr != sa:
                                delete_url = f'http://{adr}/view'
                                try:
                                    requests.delete(delete_url, json = {"socket-address" : addy}, timeout=1)
                                except:
                                    continue
                for i in ad_rm:
                    try:
                        view_list.remove(i)
                        #del vc_map[i]
                    except:
                        continue
                put_lock.release()
            t3 = threading.Thread(target=put_kvs_broadcast)
            t3.start()
            if key in kvlist.keys():
                kvlist[key] = data["value"]
                return {"result": "replaced", "causal-metadata": meta_data, "shard-id" : key_shard_assign}, 200
            else:
                kvlist[key] = data["value"]
                return {"result" : "created", "causal-metadata": meta_data, "shard-id" : key_shard_assign}, 201
        except: 
            put_lock.release()
            return {"error": "PUT request does not specify a value"}, 405
    if request.method == 'DELETE':
        try:
            data = request.get_json(force = True)
            key_hash = (int(hashlib.md5(key.encode()).hexdigest(), 16)%int(shard_count))+1
            key_shard_assign = 's' + str(key_hash) # represents the hashed shard id

            c_url = f'http://{sa}/shard/node-shard-id'
            current_shard = requests.get(c_url, timeout=1).json()["node-shard-id"]
            #If the node is not part of specified hashed shard from input key
            if current_shard != key_shard_assign:
                for ip in shard_list[key_shard_assign]:
                    try:
                        forward_url = f'http://{ip}/kvs/{key}'
                        response = requests.delete(forward_url, json = data, timeout=1)
                        put_lock.release()
                        return response.json(), response.status_code
                    except:
                        continue

            meta_data = data["causal-metadata"]
            if meta_data is not None:
                if not check_equal(meta_data, vc_map):
                    put_lock.release()
                    return {"error": "Causal dependencies not satisfied; try again later"}, 503
            if key not in kvlist.keys():
                put_lock.release()
                return {"error": "Key does not exist"}, 404
            vc_map[sa] += 1 #Message about to be sent
            meta_data = vc_map #Updating metadata to match current main replica vector clock
            #broadcast replace to every other replica
            
            ad_rm = []
            vc_map_holder = vc_map.copy()
            view_list_holder = view_list.copy()
            def delete_kvs_broadcast():
                for addy in view_list_holder:
                    if addy == sa:
                        continue
                    try:
                        broadcast_url = f'http://{addy}/kvmod/{key}'
                        requests.delete(broadcast_url, json = {"causal-metadata" : vc_map_holder, "sender": sa, "key_hash" : key_shard_assign}, timeout=1)
                    except: # if replica is not repsonding delete from all views
                        ad_rm.append(addy)
                        for adr in view_list_holder:
                            if adr != addy and adr != sa:
                                delete_url = f'http://{adr}/view'
                                try:
                                    requests.delete(delete_url, json = {"socket-address" : addy},timeout=1)
                                except:
                                    continue
                for i in ad_rm:
                    try:
                        #del vc_map[i]
                        view_list.remove(i)
                    except:
                        continue
                put_lock.release()
            t2 = threading.Thread(target=delete_kvs_broadcast)
            t2.start()
            del kvlist[key]
            return {"result": "deleted", "causal-metadata": meta_data, "shard-id" : key_shard_assign}, 200
        except:
            put_lock.release()
            return {"error": "Invalid argument"}, 405
    else: # request is GET
        try:
            data = request.get_json(force = True)
            key_hash = (int(hashlib.md5(key.encode()).hexdigest(), 16)%int(shard_count))+1
            key_shard_assign = 's' + str(key_hash) # represents the hashed shard id

            c_url = f'http://{sa}/shard/node-shard-id'
            current_shard = requests.get(c_url, timeout=1).json()["node-shard-id"]
            if current_shard != key_shard_assign:
                for ip in shard_list[key_shard_assign]:
                    try:
                        forward_url = f'http://{ip}/kvs/{key}'
                        response = requests.get(forward_url, json = data, timeout=1)
                        return response.json(), response.status_code
                    except:
                        continue
            
            meta_data = data["causal-metadata"]
            if meta_data is not None:
                if not check_equal(meta_data, vc_map):
                    return {"error": "Causal dependencies not satisfied; try again later"}, 503
            else:
                meta_data = vc_map
            if key in kvlist.keys():
                return {"result": "found", "value": kvlist[key], "causal-metadata": meta_data, "shard-id" : key_shard_assign}, 200
            else: 
                return {"error": "Key does not exist"}, 404
        except:
            return {"error": "Invalid argument"}, 400





if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8090)