import requests

def vc_combine(vc1, vc2): # combines two vector clocks
    for addy in vc1:
        if vc2[addy] > vc1[addy]:
            vc1[addy] = vc2[addy]
    return vc1
def deliverable(vc1, vc2, sa): #returns true if message can be delivered
    if vc1[sa] == (vc2[sa] + 1):
        for addy in vc1:
            if addy == sa:
                continue
            if vc1[addy] > vc2[addy]:
                return False
        return True
    else:
        return False
def optimal_insert(shard_list): #return optimal index to add new shard to
    bfr = len(shard_list["s1"])
    for shard in shard_list:
        if len(shard_list[shard]) != bfr:
            return shard
    return "s1"

def check_equal(vc1, vc2):
    set1 = set(vc1.items())
    set2 = set(vc2.items())
    d_list = list(set1 - set2)
    count = 0
    for tup in d_list:
        count += tup[1]
    if count != 0:
        return False
    return True

    
