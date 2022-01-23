import os, sys, time
import socket, redis, json, random

from pyraft import raft

base_port = 5000 

def connect(n):
	r = redis.StrictRedis(host=n.ip, port=n.port)
	return r

def connect_leader(nodes):
	for n in nodes:
		s = socket.socket()
		s.connect((n.ip, n.port))
		s.send(b'info\r\n')
		data = s.recv(4096).decode('utf-8')
		lines = data.split('\r\n')
		#print(lines)

		info = json.loads(lines[1])
		#print(info)
		s.close()

		if info['state'] == 'l':
			r = redis.StrictRedis(host=n.ip, port=n.port)
			return r

	return None


cache = {}
count = 1000

def set_test_value(nodes):
	r = connect_leader(nodes)
	for i in range(count):
		v = 'value_%07d' % random.randrange(0, 1000000)
		cache[i] = v 
		r.set('key_%d' % i, v)

	r.close()
	return True


def check_test_value(nodes):
	for n in nodes:
		r = connect(n)
		
		for i in range(count):
			v = r.get('key_%d' % i).decode('utf-8')
			if v != cache[i]:
				print('value of key_%d mismatch %s : %s' % (i, v, cache[i]))
				return False
		
	return True

		





def make_test_node_set(n_node):
	ensemble = {}
	for j in range(n_node):
		ensemble[str(j)] = '127.0.0.1:%d' % (base_port + 10*j)
	#print ensemble

	nodes = []
	for j in range(n_node):
		n = raft.RaftNode(str(j), '127.0.0.1:%d' % (base_port + 10*j), ensemble)
		nodes.append(n)

	return nodes, ensemble
	

def make_test_node(nid, peers=[]):

	ensemble = {}
	for peer in peers:
		ensemble[peer.nid] = peer.addr

	n = raft.RaftNode(str(nid), '127.0.0.1:%d' % (base_port + 10*nid), ensemble)

	return n
	

def check_state(nodes, timeout = 10):
	def _check_state(nodes, timeout):
		count = int(timeout / 0.1)
		for j in range(count):
			max_term = 0
			lcnt = 0
			fcnt = 0

			for n in nodes:
				if n.state == 'l':
					lcnt += 1
				if n.state == 'f':
					fcnt += 1

				if n.term > max_term:
					max_term = n.term
			
			flag_wait = True
			if lcnt == 1 and fcnt == len(nodes) - 1:
				flag_wait = False
				for n in nodes:
					if n.term != max_term:
						flag_wait = True
						break

			if flag_wait == False:
				break

			time.sleep(0.1)

		if lcnt != 1 or fcnt != len(nodes) - 1:
			print('voting failed. state is abnoraml %d:%d' % (lcnt, fcnt))
			return False
		else:
			for n in nodes:
				if n.term != max_term:
					print('voting failed. term mismatched %d-%d' % (max_term, n.term))
					return False

		return True


	start = time.time()
	ret = _check_state(nodes, timeout)
	end = time.time()
	print('voting done. (elapsed: %f sec)' % (end-start))

	for n in nodes:
		print('nid:%s, state:%s, term:%d, index:%d' % (n.nid, n.state, n.term, n.index))

	return ret

