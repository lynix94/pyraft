## pyraft - Python raft implementation



### pyraft

The pyraft is a python raft implementation. This implementation can be used as daemon (like consul and zookeeper). But the main purpose of this is to be integrated in python application.

You can import raft.py and extend RaftNode functions by adding handler functions or inherit it easily



### Basic usage

You can download pyraft by pip

```
pip3 install pyraft
```

You can run raft node by using run_raft.py of example. like below.

```
python3 run.py -a IP:PORT [-i NODE_ID] [-e ENSEMBLE_LIST]
  ex) python3 run_raft.py -a 127.0.0.1:5010 -i 1 -e 2/127.0.0.1:5020,3/127.0.0.1:5030
  ex) python3 run_raft.py -a 127.0.0.1:5010 -i 1 -e 127.0.0.1:5020,127.0.0.1:5030
lynix@~/lab/pyraft$ 
lynix@~/lab/pyraft$ python3 run_raft.py -i 1 -a 127.0.0.1:5010
[INFO][1-1(c):2020-04-05 17:51:46.806923] get 1. voters: ['1']
[INFO][1-1(c):2020-04-05 17:51:46.807097] 1 is a leader
[INFO][1-1(c):2020-04-05 17:51:46.807145] on_leader called
```


or run by -m option like below
```
lynix@~/lab/pyraft$ python3 -m pyraft.run_raft -i 1 -a 127.0.0.1:5010
[INFO][1-1(c):2020-04-13 01:13:38.982386] get 1. voters: ['1']
[INFO][1-1(c):2020-04-13 01:13:38.982443] 1 is a leader
[INFO][1-1(c):2020-04-13 01:13:38.982839] on_leader called
```


-i is a node id and it will be same as -a if its omitted. -a is the address of this node. The pyraft use port to listen client and use port + 1 for internal raft processing.

-e is the comma separated ensemble lists. NID/IP:PORT is the format of other node.



You can run node 2 and 3 like below

```
lynix@~/lab/pyraft$ python3 run_raft.py -i 2 -a 127.0.0.1:5020 -e 1/127.0.0.1:5010,3/127.0.0.1:5030
[INFO][2-0(c):2020-04-05 18:19:32.689253] connect to 1
...
[INFO][2-11(f):2020-04-05 18:20:05.292156] connect to 3 ok
```



```
lynix@~/lab/pyraft$ python3 run_raft.py -i 3 -a 127.0.0.1:5030 -e 1/127.0.0.1:5010,2/127.0.0.1:5020
[INFO][3-0(c):2020-04-05 18:20:04.443755] connect to 1
[WARN][3-0(c):2020-04-05 18:20:04.444187] node 1 already exists
[WARN][3-0(c):2020-04-05 18:20:04.444228] node 3 already exists
...
[INFO][3-1(c):2020-04-05 18:20:05.292312] get 1. voters: ['3']
[INFO][3-2(c):2020-04-05 18:20:06.245078] get 1. voters: ['3']
[INFO][3-11(c):2020-04-05 18:20:06.292755] on_follower called
```



The pyraft provides get, set, del, expire commands with the redis protocol (RESP) interface. You can read from all of nodes but you should write to master node (relay from follower is not implemented yet)

Below is an example. 

```
lynix@~/lab/pyraft$ telnet localhost 5010
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
set key value
+OK
get key
+value


lynix@~/lab/pyraft$ telnet localhost 5020
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
get key
+value
```

 

### Dynamic node management



The pyraft node can be run without ensemble context. It'll run as candidate alone if there is no ensemble option. If ensemble option is added, It send its information to node and receive previous ensemble context at that time.

So you can run first node like below. (without ensemble option)

```
lynix@~/lab/pyraft$ python3 run_raft.py -i 1 -a 127.0.0.1:5010
[INFO][1-1(c):2020-04-05 22:48:42.412140] get 1. voters: ['1']
[INFO][1-1(c):2020-04-05 22:48:42.412275] 1 is a leader
[INFO][1-1(c):2020-04-05 22:48:42.412336] on_leader called

```

Then node one vote and make itself as leader.



Then second node can be run like below. it names itself as node 2 and request to node 1 ensemble info.

Then it receives there are node 1 and 2 only and get ping from leader. And it turns to follower. 

```
lynix@~/lab/pyraft$ python3 run_raft.py -i 2 -a 127.0.0.1:5020 -e 127.0.0.1:5010
[INFO][2-0(c):2020-04-05 22:48:58.663016] connect to __TEMP_127.0.0.1:5010__
[WARN][2-0(c):2020-04-05 22:48:58.663430] node 2 already exists
...
[INFO][2-5(c):2020-04-05 22:49:00.418973] get 1. voters: ['2']
[INFO][2-11(c):2020-04-05 22:49:00.419175] on_follower called
```



Node 3 can be run like below. it also names itself as node 3 and request to node 2 (or node 1 also possible) ensemble info. Then it gets node 1, 2 and 3 as ensemble info. Node 1 and 2 also expand ensemble with node 3.

```
lynix@~/lab/pyraft$ python3 run_raft.py -i 3 -a 127.0.0.1:5030 -e 127.0.0.1:5020
[INFO][3-0(c):2020-04-05 22:49:10.616798] connect to __TEMP_127.0.0.1:5020__
[WARN][3-0(c):2020-04-05 22:49:10.617187] node 3 already exists
...
[INFO][3-3(c):2020-04-05 22:49:12.239592] get 1. voters: ['3']
[INFO][3-4(c):2020-04-05 22:49:12.425958] get 1. voters: ['3']
[INFO][3-11(c):2020-04-05 22:49:12.426090] on_follower called
```



### logging and snapshot

The pyraft node make its checkpoint in every 100000 log entries. (can be changed by raft.CONF_LOG_MAX)

Or generate checkpoint snapshot by 'checkpoint' comand. Then it writes its data to raft_NODENAME_TIMESTAMP_INDEX.dat. The pyraft node also writes append_entry log to raft_NODENAME_INDEX.log to get persistence.

The snapshot date is now made by expr() of python. And you can read data by editor. (It will be use pickle later)

The first pyraft node can start from snapshot by -l option. But other node can get snapshot data from leader node.



### Extend RaftNode functions

The pyraft has RaftWorker class in it to handle user request (get, set, del, expire, expreat ...). RaftWorker get user request and find it in its handler table. (RaftWorker.handler)

You can add other kind of commands in it to handle other user requests like below

```
n.worker.handler['lrange'] = [do_lrange, 'r', 0, 2]
```

do_lrange is callback function and 'r' means read only (do not invoke append_entry), 0 and 2 is minimum and maximum parameter of this function.

You can add on_leader, on_follower and on_candidate callback if you handle node change event.

Below is url check example shows this.

```
def url_checker(node):
	while not node.shutdown_flag:
		time.sleep(5)

		if node.state != 'l':
			continue

		for k, v in node.data.items():
			if not k.startswith('url_'):
				continue

			try:
				url = v
				if not v.startswith('http'):
					url = 'https://' + v

				result = urllib.request.urlopen(url).read()
				print('url %s is ok' % k) 
			except Exception as e:
				print('url %s is bad - %s' % (k, e))


def url_check_start(node):
	print('url check start...')
	if not hasattr(node, 'checker'):
		node.checker = threading.Thread(target=url_checker, args=(node,))
		node.checker.start()
	

node = raft.make_default_node()

node.worker.handler['on_leader'] = url_check_start

node.start()
node.join()


```



When you start this example (run_ex1.py file in the project). It checks url is available or not in every 5 seconds. Only the leader of ensemble node checks this and if leader is down. New elected leader starts checking by 'on_leader' callback.



Like above example, you can integrate raft consensus in your own application easily. And you can also inherit RaftNode if you want to make more complex raft integrated application.



I hope this project is useful to make raft integrated application. And any kind of questions or contributions are welcome. 

Thanks.




