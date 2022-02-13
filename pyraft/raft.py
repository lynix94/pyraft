import time, threading, socket, select
import random, queue
from datetime import datetime

from pyraft.common import *
from pyraft import resp
from pyraft.log import RaftLog
from pyraft.log import LogItem
from pyraft.worker import RaftWorker

class RaftNode(object):
	def __init__(self, nid, addr, ensemble={}, peer = False):
		# raft node & peer common
		self.nid = nid
		self.term = 0
		self.index = 0
		self.state = 'c'
		self.last_append_entry_ts = 1
		self.last_delayed_ts = 1
		self.last_checkpoint = 0
		self.first_append_entry = False
		self.last_applied = 0
		self.commit_index = 0

		self.addr = addr
		self.ip, self.port = addr.split(':', 1)
		self.port = int(self.port)

		self.raft_req = resp.resp_io(None)
		self.raft_wait = resp.resp_io(None)

		if peer == True:
			return

		# raft node only
		self.shutdown_flag = False

		self.peers = {}
		self.peer_lock = threading.Lock()

		self.log = RaftLog(nid)
		self.worker = RaftWorker()
		self.data = {}
		self.data_lock = threading.Lock()
		self.data['ttl'] = {}
		self.ttl = self.data['ttl']

		for pid, paddr in ensemble.items():
			if pid == nid:
				continue

			self.add_node(pid, paddr)

	def get_handler(self, name):
		if name not in self.worker.handler:
			return None

		return self.worker.handler[name]

	def get_handler_func(self, name): # return function only
		handler = self.get_handler(name)
		if isinstance(handler, list):
			return handler[0]

		return handler

	def propose(self, cmd):
		handler = self.get_handler(cmd[0].lower())
		if handler is None:
			raise Exception('unknown commands: %s' % cmd)

		if 'e' in handler[1]:
			if self.state == 'c':
				self.log_warn('request while candidate')
				raise Exception('temporary unavailable')

			if self.state != 'l':
				for nid, p in self.get_peers().items():
					if p.state == 'l':
						return self.worker.relay_cmd(p, cmd)

				raise Exception('cannot relay to leader')

			f = Future(cmd)
			self.q_entry.put(f)

			ret = f.get(10)
			if ret == ERROR_APPEND_ENTRY:
				self.log_info('append_entry failed (%s)' % str(words))
		else:
			ret = handler[0](self, cmd)

		return ret

	def apply_loop(self):
		i = 0
		while True:
			if self.shutdown_flag:
				break

			if i % 10 == 0:
				# print self.get_snapshot()
				pass
			i += 1

			if self.log.size() > CONF_LOG_MAX:
				self.checkpoint()

			item = self.log.pop(1)
			if item == None:
				continue

			cmd = item.cmd
			if isinstance(cmd, Future):
				cmd = cmd.cmd

			if self.index >= item.index:
				self.log_info('skip log [%d:%d]: "%s"' % (self.index, item.index, str(cmd)))
				continue

			self.log_debug('apply command [%d]: "%s"' % (item.index, str(cmd)))

			handler = self.get_handler(cmd[0].lower())
			if handler is None:
				self.log_error('unknown command: %s' % cmd)
				sys.exit(-1)

			with self.data_lock:
				try:
					ret = handler[0](self, cmd)
				except Exception as e:
					ret = e

				self.index = item.index

			if isinstance(item.cmd, Future):
				item.cmd.set(ret)

	def load(self, filename):
		self.log_info('nid %s load %s' % (self.nid, filename))
		try:
			fh = open(filename, 'r')
			data = fh.read()
			fh.close()
		except IOError as e:
			self.log_error('failed to load: %s' % str(e))
			return False

		self.data = eval(data)
		meta = self.data['_META_']
		meta['id'] = self.nid
		meta['state'] = self.state
		self.term = meta['term']
		self.index = meta['index']
		self.log.index = self.index

		while True:
			try:
				fh = open('raft_%s_%010d.log' % (self.nid, self.index+1))
				remain = fh.read()
				fh.close()
			except IOError:
				break


			while True:
				l, remain = resp.decoding(remain)
				if l == None:
					break

				# term, index, ts, cmd
				index = l[1]
				cmd = l[3]

				handler = self.get_handler(cmd[0].lower())
				if handler is None:
					self.log_error('unknown command: %s' % cmd)
					sys.exit(-1)

				try:
					handler[0](self, cmd)
				except Exception:
					pass

				self.index = index
				self.log.index = self.index

				if remain == '':
					break

		return True

	def start(self):
		self.shutdown_flag = False

		self.q_entry = queue.Queue(4096)

		self.th_raft = threading.Thread(target = self.raft_listen)
		self.th_raft.start()

		self.th_le = threading.Thread(target = self.leader_election)
		self.th_le.start()

		self.th_apply = threading.Thread(target=self.apply_loop)
		self.th_apply.start()

		self.worker.start(self)
		self.on_start()

	def shutdown(self):
		self.worker.shutdown()
		self.shutdown_flag = True
		self.on_shutdown()

	def join(self):
		self.th_raft.join()
		self.th_le.join()
		self.th_apply.join()

		self.worker.join()

		for nid, peer in self.get_peers().items():
			peer.raft_req.close()
			peer.raft_wait.close()

		self.log.close()

	def add_node(self, nid, addr):
		with self.peer_lock:
			if nid == self.nid or nid in self.peers:
				self.log_warn('node %s already exists' % nid)
				return False

			if '__TEMP_%s__' % addr in self.peers: # replace temp peer
				del self.peers['__TEMP_%s__' % addr]

			for pid, peer in self.peers.items():
				if addr == peer.addr:
					self.log_warn('address %s already used in node %s' % (addr, pid))
					return False

			self.peers[nid] = RaftNode(nid, addr, peer = True)

		#self.raft_connect()
		return True

	def del_node(self, nid):
		with self.peer_lock:
			if nid not in self.peers:
				self.log_error('node %s not exists' % nid)
				return

			p = self.peers[nid]
			p.raft_req.close()
			p.raft_wait.close()
			del self.peers[nid]

	def get_peers(self):
		ret = {}
		with self.peer_lock:
			for nid, peer in self.peers.items():
				ret[nid] = peer
			
		return ret

	def raft_connect(self):
		for nid, peer in self.get_peers().items():
			if peer.raft_req.connected():
				continue

			try:
				sock = socket.socket()
				sock.connect((peer.ip, peer.port+1))
			except socket.error:
				sock.close()
				continue

			peer.raft_req = resp.resp_io(sock)
			self.log_info('connect to %s' % (nid))
			peer.raft_req.raw_write('id %s %s %d' % (self.nid, self.addr, self.index))

			peers = peer.raft_req.read(1)
			if not isinstance(peers, list):
				self.log_warn('connect to %s failed: "%s"' % (nid, str(peers)))
				return

			for p in peers:
				toks = p.split('/', 1)
				self.add_node(toks[0], toks[1])

			self.log_info('connect to %s ok' % nid)


	def process_raft_accept(self, sock):
		nid = None
		
		rio = resp.resp_io(sock)
		words = rio.read(1)
		if words == None or words == '':
			rio.close()
			return

		if isinstance(words, str):
			words = words.split()

		if len(words) == 4 and words[0] == 'id':
			nid = words[1]
			addr = words[2]
			index = intcast(words[3])
			if index == None:
				self.log_error('invalid id: %s', words)
				return

		self.log_info('raft accept: %s' % nid)

		if nid != None: # new peer
			if nid not in self.peers: # new node
				ret = self.add_node(nid, addr)
				if ret == False:
					rio.write(Exception('cannot add node (invalid nid or exists)'))
					rio.close()
					return
					
			peer = self.peers[nid]
			if peer.addr != addr:
				rio.write(Exception('nid already in ensemble'))
				rio.close()
				return
			else:
				# reconnect
				if peer.raft_wait != None:
					peer.raft_wait.close()

				peer.raft_wait = rio
				peers = ['%s/%s' % (self.nid, self.addr)]
				for nid, p in self.get_peers().items():
					peers.append('%s/%s' % (nid, p.addr))

				peer.raft_wait.write(peers)
				#self.log_info('peer write to %s' % peer.nid)
		else:
			self.log_error('invalid raft command: %s' % words)
			rio.write(Exception('invalid raft command'))
			rio.close()
			
	def raft_listen(self):
		self.raft_listen_sock = socket.socket()
		self.raft_listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.raft_listen_sock.bind((self.ip, self.port+1))
		self.raft_listen_sock.listen(1)
		self.raft_listen_sock.settimeout(1)

		while True:
			try:
				sock, addr = self.raft_listen_sock.accept()
				self.process_raft_accept(sock)
			except socket.timeout:
				if self.shutdown_flag:
					self.raft_listen_sock.close()
					break

	#
	# leader election
	#
	def leader_election(self):
		while True:
			self.raft_connect()

			if self.state == 'f':
				self.do_follower()
			elif self.state == 'c':
				self.do_candidate()
			elif self.state == 'l':
				self.do_leader()
			else:
				self.log_error('unknown state: %s' % self.state)

			if self.shutdown_flag:
				for nid, peer in self.get_peers().items():
					peer.raft_req.close()
				break

	def set_leader(self, node):
		if node.nid == self.nid:
			if self.state != 'l':
				self.first_append_entry = True
				self.on_leader()

			self.state = 'l'
		else:
			if self.state != 'f':
				self.on_follower()

			self.state = 'f'

		for nid, peer in self.get_peers().items():
			if node.nid == nid:
				peer.state = 'l'
			else:
				peer.state = 'f'

	def select_peer_req(self, timeout):
		sock_peer_map = {}
		for nid, p in self.get_peers().items():
			if p.raft_wait.sock != None:
				sock_peer_map[p.raft_wait.sock] = p

		if len(sock_peer_map) == 0:
			return []

		reads, writes, excepts = select.select(list(sock_peer_map.keys()), [], [], timeout)
		peers = []
		for r in reads:
			peers.append(sock_peer_map[r])
			
		return peers

	def handle_request(self, p, toks):
		#self.log_debug('handle req: %s' % str(toks))
		if toks[0] != 'append_entry' and toks[0] != 'snapshot':
			self.log_info('unknown or delayed request from %s: %s' % (p.nid, toks))
			return False

		term = intcast(toks[1])
		prev_term = intcast(toks[2])
		prev_index = intcast(toks[3])
		commit_index = intcast(toks[4])
		if term == None or prev_term == None or prev_index == None or commit_index == None:
			self.log_error('invalid append_entry: %s' % toks)
			return False

		if term < self.term:
			self.log_info('old term request from %s: %s' % (p.nid, toks))
			return False

		self.term = term
		self.set_leader(p)

		if self.commit_index != commit_index:
			self.commit_index = commit_index
			self.log.apply_commit_index(commit_index)

		if toks[0] == 'append_entry': # append_entry, term, prev_term, prev_index, commit_index, ts, cmds...
			ts = toks[5]
			if len(toks) > 6:
				self.log_debug('apply append_entry to %d-%d' % (term, prev_index))
				index = prev_index + 1
				item = LogItem(self.term, index, ts, toks[6:])
				self.log.push(item, self.commit_index)
			else:
				index = self.index
		elif toks[0] == 'snapshot': # snapshot, term, prev_term, prev_index, commit_index, data
			self.log_info('apply snapshot to %d-%d' % (term, prev_index))
			self.data = eval(toks[5])
			meta = self.data['_META_']
			meta['id'] = self.nid
			meta['state'] = self.state
			self.term = meta['term']
			self.index = meta['index']
			self.log.index = self.index
			index = self.index

		p.raft_wait.write('ack %d' % index)
		self.last_append_entry_ts = int(time.time())
		return True

	def handle_ack(self, p, expect = 0, timeout = 0.0):
		start = time.time()
		while True:
			now = time.time()
			if timeout > 0 and now - start > timeout:
				break
				
			msg_list = p.raft_req.read_all(0.0)
			if msg_list == None:
				return

			for toks in msg_list:
				if isinstance(toks, str):
					toks = toks.split()

				if toks[0] == 'ack':
					index = intcast(toks[1])
					if index == None:
						self.log_error('invalid ack: %s' + toks)

					p.state = 'f'
					p.term = self.term
					p.index = index
					p.last_append_entry_ts = time.time()
				else:
					self.log_info('unknown append_entry resp. from %s: "%s"' % (p.nid, toks))

			if p.index >= expect:
				break



	def do_follower(self):
		#self.log_info('do_follower')

		peers = self.select_peer_req(1.0)
		for p in peers:
			msg_list = p.raft_wait.read_all()
			if msg_list == None or msg_list == []:
				continue

			for toks in msg_list:
				if isinstance(toks, str):
					toks = toks.split()

				if toks[0] == 'vote':
					term = intcast(toks[1].strip())
					if term == None:
						self.log_error('invalid vote: %s' % toks)
						continue

					if term > self.term:
						p.raft_wait.write('yes')
					else:
						p.raft_wait.write('no')
				else:
					old_term = self.term
					self.handle_request(p, toks)
					if self.term > old_term:
						# split brain & new leader elected. 
						# clean data to install snapshot in case of async mode
						self.index = 0
						return

		if self.last_append_entry_ts > 0 and int(time.time()) - self.last_append_entry_ts > CONF_PING_TIMEOUT:
			self.on_candidate()
			self.state = 'c'


	def do_candidate(self):
		if len(self.get_peers()) > 0:
			connected = 0
			for nid, p in self.get_peers().items():
				if p.raft_req.connected():
					connected += 1
			if connected == 0:
				return

		#self.log_info('do_candidate')
		self.term += 1

		voting_wait = CONF_VOTING_TIME * 0.1
		vote_wait_timeout = random.randint(0, CONF_VOTING_TIME*1000  * 0.5) / 1000.0
		wait_remaining = 1 - vote_wait_timeout
		voted = False

		# process vote
		peers = self.select_peer_req(vote_wait_timeout)
		for p in peers:
			msg_list = p.raft_wait.read_all()
			if msg_list == None or msg_list == []:
				continue

			for toks in msg_list:
				if isinstance(toks, str):
					toks = toks.split()

				if toks[0] == 'vote':
					term = intcast(toks[1].strip())
					if term == None:
						self.log_error('invalid vote: %s' % toks)
						continue

					if not voted and term >= self.term:
						p.raft_wait.write('yes')
						voted = True
						self.term = term
					else:
						if term >= self.term:
							self.term = term
							
						p.raft_wait.write('no')
				else:
					if self.handle_request(p, toks):
						return # elected

		if voted:
			for nid, p in self.get_peers().items():
				msg_list = p.raft_wait.read_all(wait_remaining)
				if msg_list == None or msg_list == []:
					continue

				for toks in msg_list:
					if isinstance(toks, str):
						toks = toks.split()

					if self.handle_request(p, toks):
						return # elected

			return # not elected try next

		
		# process vote request
		count = 1
		voters = [self.nid]
		for nid, p in self.get_peers().items():
			p.raft_req.write('vote %d' % self.term)
		
		for i in range(2):
			get_result = {}
			for nid, p in self.get_peers().items():
				if nid in get_result:
					continue

				msg_list = p.raft_req.read_all(i*(CONF_VOTING_TIME/2))
				if msg_list == None or msg_list == []:
					continue

				for toks in msg_list:
					if isinstance(toks, str):
						toks = toks.split()

					if toks[0] == 'yes':
						voters.append(nid)
						count+=1
						get_result[nid] = True
					elif toks[0] == 'no':
						get_result[nid] = False
					else:
						self.handle_request(p, toks)

		# process result
		self.log_info('get %d. voters: %s' % (count, str(voters)))
		if count > (len(self.peers)+1)/2:
			self.log_info('%s is a leader' % (self.nid))
			self.set_leader(self)
			self.term += 10 


	def append_entry(self, future):
		ts = time.time()
		prev_index = self.log.get_index()
		prev_term = self.log.get_term()

		if future != None:
			append_cmd = ['append_entry', self.term, prev_term, prev_index, self.commit_index, ts]
			append_cmd += future.cmd
			for nid, p in self.get_peers().items():
				self.log_debug('leader write to %s: "%s"' % (p.nid, str(append_cmd)))
				p.raft_req.write(append_cmd)

			n_nodes = len(self.get_peers()) + 1
			half = n_nodes / 2.0
			n_ack = 1 # 1 for me

			for nid, p in self.get_peers().items():
				self.handle_ack(p, expect=prev_index+1, timeout=1.0)
				if p.index == prev_index+1:
					n_ack+=1

			if n_ack > half:
				self.commit_index = prev_index+1
				item = LogItem(self.term, prev_index+1, ts, future)
				self.log.push(item, self.commit_index)
				# send dummy append below to noti commit
			else:
				future.set(ERROR_APPEND_ENTRY)
				return

		append_cmd = ['append_entry', self.term, prev_term, prev_index, self.commit_index, ts]
		for nid, p in self.get_peers().items():
			p.raft_req.write(append_cmd)

	def get_pending_time(self): # get max diff ack time
		if self.state != 'l':
			return None # cannot determine

		now = time.time()
		max_diff = 0
		for nid, p in self.get_peers().items():
			if p.state == 'f':
				diff = now - p.last_append_entry_ts
				if diff > max_diff:
					max_diff = diff

		return max_diff

	def do_leader(self):
		#self.log_info('do_leader')
		for nid, p in self.get_peers().items():
			self.handle_ack(p)

		for nid, p in self.get_peers().items():
			now = time.time()
			if p.index == self.index:
				p.last_delayed_ts = now
				continue

			if now - p.last_delayed_ts > 2.0 and p.raft_req.connected() and p.index < self.index:
				p.last_delayed_ts = now
				self.process_install_snapshot(p)

		try:
			if self.first_append_entry:
				self.first_append_entry = False
				item = self.q_entry.get(False)
			else:
				item = self.q_entry.get(True, 1.0)
		except queue.Empty:
			item = None

		self.append_entry(item)

		# read peer request if exists
		peers = self.select_peer_req(0.0)
		for p in peers:
			msg_list = p.raft_wait.read_all()
			if msg_list == None or msg_list == []:
				continue

			for toks in msg_list:
				if isinstance(toks, str):
					toks = toks.split()

				if toks[0] == 'vote':
					p.raft_wait.write('no')
				else:
					old_term = self.term
					self.handle_request(p, toks)
					if self.term > old_term:
						# split brain & new leader elected. 
						# clean data to install snapshot in case of async mode
						self.index = 0
						return


	def get_snapshot(self):
		meta = {}
		meta['id'] = self.nid
		meta['term'] = self.term
		meta['index'] = self.index
		meta['state'] = self.state

		ensemble = {self.nid:self.addr}
		for nid, p in self.get_peers().items():
			ensemble[nid] = p.addr
		meta['ensemble'] = ensemble

		self.data['_META_'] = meta

		return self.data.__repr__()


	def checkpoint(self, filename=None):
		data = self.get_snapshot()
		flag_cleanup = False
		if filename == None:
			flag_cleanup = True
			filename = 'raft_%s_%d_%d.dat' % (self.nid, int(time.time()), self.index)

		fh = open(filename, 'w')
		fh.write(self.data.__repr__())
		fh.close()

		self.last_checkpoint = self.index
		
		if flag_cleanup:
			self.log.cleanup(self.index)

	def process_install_snapshot(self, p):
		diff = self.index - p.index
		prev_index = self.log.get_index()
		prev_term = self.log.get_term()


		if p.index < self.log.start_index() or (diff >= 100 or diff > len(self.data)/10):
			snapshot = self.get_snapshot()
			self.log_info('send snapshot to %s(%d)' % (p.nid, p.index))
			p.raft_req.write(['snapshot', self.term, prev_term, prev_index, self.commit_index, snapshot])
		else:
			old_logs = self.log.get_range(p.index) # term, index, ts, commands
			for l in old_logs:
				self.log_info('send append_entry to %s(%d)' % (p.nid, p.index))
				p.raft_req.write(['append_entry', self.term, l.term, l.index-1, self.commit_index, l.ts] + l.cmd)

	#
	# changed plugin. inherit or modify this (or add handler)
	#
	def on_start(self):
		self.log_info('on_start called')
		handler = self.get_handler_func('on_start')
		if handler is not None:
			handler(self)

	def on_shutdown(self):
		self.log_info('on_shutdown called')
		handler = self.get_handler_func('on_shutdown')
		if handler is not None:
			handler(self)

	def on_leader(self):
		self.log_info('on_leader called')
		handler = self.get_handler_func('on_leader')
		if handler is not None:
			handler(self)

	def on_follower(self):
		self.log_info('on_follower called')
		handler = self.get_handler_func('on_follower')
		if handler is not None:
			handler(self)

	def on_candidate(self):
		self.log_info('on_candidate called')
		handler = self.get_handler_func('on_candidate')
		if handler is not None:
			handler(self)
		
	#
	# log, etc
	#
	def log_debug(self, msg):
		if get_log_level() <= 0:
			log = '[DEBUG][%s-%d(%s):%s] %s\n' % (self.nid, self.term, self.state, datetime.now(), msg)
			log_write(log)

	def log_info(self, msg):
		if get_log_level() <= 1:
			log = '[INFO][%s-%d(%s):%s] %s\n' % (self.nid, self.term, self.state, datetime.now(), msg)
			log_write(log)

	def log_warn(self, msg):
		if get_log_level() <= 2:
			log = '[WARN][%s-%d(%s):%s] %s\n' % (self.nid, self.term, self.state, datetime.now(), msg)
			log_write(log)

	def log_error(self, msg):
		if get_log_level() <= 3:
			log = '[ERROR][%s-%d(%s):%s] %s\n' % (self.nid, self.term, self.state, datetime.now(), msg)
			log_write(log)


	def check_ttl(self, key):
		if key in self.ttl:
			ttl = self.ttl[key]
			if ttl < time.time():
				del self.ttl[key]

				if key in self.data:
					del self.data[key]

	def clear_ttl(self, key):
		if key in self.ttl:
			del self.ttl[key]
			
	def set_ttl(self, key, ts):
		if key in self.data:
			self.ttl[key] = ts
			return True
		else:
			return False

	def request(self, *cmd):
		try:
			ret = self.propose(cmd)
		except Exception as e:
			ret = e

		return ret


from optparse import OptionParser

def make_default_node():
	parser = OptionParser()
	parser.add_option('-e', '--ensemble', dest='ensemble', help='ensemble list')
	parser.add_option('-a', '--addr', dest='addr', help='ip:port[port+1]')
	parser.add_option('-i', '--nid', dest='nid', help='self node id')
	parser.add_option('-l', '--load', dest='load', help='checkpoint filename to load')


	(options, args) = parser.parse_args()

	#print options

	if options.addr == None:
		print('python3 %s -a IP:PORT [-i NODE_ID] [-e ENSEMBLE_LIST] [-l CHECKPOINT_FILE]' % sys.argv[0])
		print('  ex) python3 %s -a 127.0.0.1:5010 -i 1 -e 2/127.0.0.1:5020,3/127.0.0.1:5030' % sys.argv[0])
		print('  ex) python3 %s -a 127.0.0.1:5010 -i 1 -e 127.0.0.1:5020,127.0.0.1:5030' % sys.argv[0])
		sys.exit(-1)

	if options.nid == None:
		options.nid = options.addr

	ensemble = {}

	if options.ensemble != None:
		toks = options.ensemble.split(',')
		for tok in toks:
			etoks = tok.split('/')
			if len(etoks) == 2:
				nid = etoks[0]
				addr = etoks[1]
				ensemble[nid] = addr
			elif len(etoks) == 1:
				addr = tok
				ensemble['__TEMP_%s__' % addr] = addr
			else:
				print('invalid ensemble format')
				sys.exit(-1)

	node = RaftNode(options.nid, options.addr, ensemble)
	
	if options.load != None:
		node.load(options.load)

	return node
	
