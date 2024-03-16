## Team Contribution
Aneesh Thippa: Worked on adding sharding functionality to existing functionalitly and worked on resharding mechanism<br>
Matthew Lo:  Worked on initializing shards at startup and added sharding functionality to existing functinoality from assignment 3<br>
Primarily worked synchronously

## Acknowledgments
Shaan Mistry: Helped with edge case testing and ways to implement the resharding mechanism<br>
Joost "Will" Vonk (Tutor): Helped clarify what we can assume for sharding initialization <br>
Yan Tong: Helped with clarifying what to do with metadata in the event of a disconnection or deletion of a replica

## Citations
Flask Documentation: https://flask.palletsprojects.com/en/3.0.x/ <br>
We used the Flask Documentation to help us learn how to use syntax and theory behind the Flask API. We implemented REST APIs with Flask
<br>
Requests library documentation: https://requests.readthedocs.io/en/latest/ <br>
We used the Requests library documentation to help us get familiar with how to use HTTP requests. We used this in our system to create REST APIs
<br>
Threading in Python: https://realpython.com/intro-to-python-threading <br>
We used the Threading library to learn how to run seperate threads in order to not block flow. We also learned how to implement mutexes to ensure race conditions could not occur. We used this in our code to send broadcasts without inturrupting the returns of the main KVS functions and also to prevent race conditions when the client makes lots of back to back requests
<br>
Hashing in Python: https://docs.python.org/3/library/hashlib.html <br>
We used hashlib library documentation in order to learn how to hash certain keys. We used this in our system to hash the keys to different shards

## Mechanism Description
### Tracking Causal Dependencies: <br>
We incorporate a slightly modified casual broadcast protocol in our distributed system. Our system utilizes a hash map that represents the vector clock of our running proccesses, with the key being the IP address and the value being the number of messages broadcasted by the replica in that IP. Each process will have their own vector clock. When a new replica is added, in order to maintain the causal broadcast protocol correctly we import the vector clock of another instance to the new instance. When a replica recieves a PUT or DELETE request that is dependent on another request,not null, we check if that message is deliverable using the method we learned in class namely, a message m is deliverable at process p if VC(m)\[k\] = VC(p)\[k\] + 1 where k is the senders position, and VC(m)\[k\] <= VC(p)\[k\]. If a message cant be delivered we return 503, otherwise we deliver the message and broadcast that message to all other active replicas from which would check if that message can be delivered and otherwise add it to a queue. The algorithm when broadcasting this message if deliverable is simple as we just increment the vector clock in the senders position by one and then broadcast. We implemented a reciever side queue which tries to deliver messages from the queue whenever a new request arrives to the replica. If the request from the client is null then we adapt the metadata of the replica back to the client and continue delivery as usual.<br><br>

### Detection of a Down replica: <br>
Whenever a replica recieves a PUT or DELETE request from the client, the replica broadcasts that request to all other replicas. If the replica doing the broadcasting recieves a timeout from another replica, meaning that instance is down or disconnected, a delete request is broadcasted to all other replicas to delete the unresponsive view from the view list. If a replica were to be disconected and then a put request was made to the kvs endpoint and then the node was reconnected moments later, this may lead to a false negetive as we would remove it from the viewlist but never add it back as it was only dissconnected and we never implemented a mechanism to add back reconnected instances. If we were to disconnect a node and check the view list it would still contain the node as if it were active as we only update the viewlist when doing kvs operations or resharding, leading to a false positive. <br><br>

### Distributing nodes to shards <br>
At startup, the client initializes the server with a series of replicas that each provide data of how many shards (SHARD_COUNT) the key-value store should utilize. We assume that all the replicas in the initial view list are going to be intialized and determine how many replicas will be at startup. We have a function that does a round-robin approach to distributing the replicas among the given shard amount. We store which replicas belong to which shard in a dictionary with shards as keys and a list of replicas as the respective value. We used the round robin approach as it was simple and effective at evenly distributing nodes across the given number of shards. <br><br>

### Key-to-shard mapping <br>
We assign keys to different shards by utiliziling a hash function. We utilized the hashlib to create a hash function that divides the keys amongst the N different shards that the client specifies. This hash function is consistent and not randomized so it will work across different iterations (different replicas). Depending on the request that was made to the KVS endpoint we either continue on the current replica or we forward this request to another replica which the current key has hashed to. The KVS function will broadcast requests to every Node in the viewlist but at delivery in the recieving node we only update the key value store if the keys Hashed shard matches with the recieving replicas. We do a broadcast here and not a multicast in order to keep a consistent vector clock throughout all the nodes. We used the hashing here in order to effectively and evenly map keys to shards.

### Resharding the system <br>
Since we are not guranteed that all nodes will be active at this moment we check and remove all disconnected nodes at this time and update the viewlist of all nodes. Then we check if a reshard meeting the minimum number of nodes in each shard is possible. If it is possible then our approach was a straight forward brute force method of resharding. We reorganize the nodes based on the new shardcount dividing them up evenly using a round robin approach. Then we go through the viewlist and adapt each indiviudal key-value pair into a dictionary that is organized based on the hash of the key (Note: this approach is unrealistic as we need to collect all the data in one node but for the prupose of simplicity we incorporated it this way). After organizing each key-value pair into the dictionary seperated by the key hash and hence by the shard names, we send these groups of key-value pairs to the respective nodes that are contained in the shards. We do this by incorporating a private endpoint which takes in the new key-value store, shard_count, and shard_list, then updates it within that node. With that the resharding is complete. 