import os, sys, time, copy
import threading, argparse
import asyncio

from pyraft import raft
from kazoo.client import KazooClient

class ArcusMonitor(raft.RaftNode):
	def __init__(self, nid, addr, ensemble, zk_addr, service_code):
		super(ArcusMonitor, self).__init__(nid, addr, ensemble)

		self.zk_addr = zk_addr
		self.service_code = service_code
		self.repeated_fail_map = {}
		self.failover_thread_hold = 5
		self.cool_down_time = 0
		self.failover_count = 0
		self.failover_count_limit = 3

	def do_failover(self, addr):
		self.log_info('process failover: %s' % addr)

		try:
			self.log_info('## delete /arcus/cache_list/%s/%s' % (self.service_code, addr))
			self.zk.delete('/arcus/cache_list/%s/%s' % (self.service_code, addr))
			self.failover_count += 1

		except Exception as e:
			self.log_info('node %s is already deleted (maybe gracefully shutdown): %s' % (addr, str(e)))

	def do_failback(self, addr):
		# nothing to do in ArcusMonitor
		pass

	def do_make_decision(self):

		for addr in copy.deepcopy(self.check_list):
			result = self.request('hgetall', 'hc_%s' % addr)
			opinions = {}
			for i in range(0, len(result), 2): # k, v parsing
				nid = result[i]
				repeated_fail = result[i+1]
				opinions[nid] = repeated_fail

			voted = 0.0
			for nid, repeated_fail in opinions.items():
				if repeated_fail >= self.failover_thread_hold:
					voted += 1.0

			if voted / len(opinions) > 0.666:
				self.do_failover(addr)

	async def check_arcus(self, addr):
		#print('check arcus: ', addr)
		ip, port = addr.split('-', 1)[0].split(':')

		if addr not in self.repeated_fail_map:
			self.repeated_fail_map[addr] = 0
			self.request('hset', 'hc_%s' % (addr), self.nid, 0)

		repeated_fail = self.repeated_fail_map[addr]

		try:
			fut = asyncio.open_connection(ip, int(port))
			reader, writer = await asyncio.wait_for(fut, timeout=1.0)
		except Exception as e:
			self.log_error('connect to %s failed: %s' % (addr, str(e)))
			self.repeated_fail_map[addr] = repeated_fail + 1
			self.request('hset', 'hc_%s' % (addr), self.nid, repeated_fail + 1)
			return

		try:
			writer.write('get __key__\r\n'.encode())
			fut = reader.readuntil('END\r\n'.encode())
			data = await asyncio.wait_for(fut, timeout=1.0)
			writer.write('quit\r\n'.encode())
			writer.close()

		except Exception as e:
			self.log_error('request to %s failed: %s' % (addr, str(e)))
			self.repeated_fail_map[addr] = repeated_fail + 1
			self.request('hset', 'hc_%s' % (addr), self.nid, repeated_fail + 1)
			return

		if self.repeated_fail_map[addr] != 0: # reset if needed only (efficiency)
			self.repeated_fail_map[addr] = 0
			self.request('hset', 'hc_%s' % (addr), self.nid, 0)

	async def check_all(self):
		last_printed_ts = 0
		while not self.shutdown_flag:
			time.sleep(1.0)

			jobs = []
			if self.hc_flag == 'stop':
				continue

			if self.hc_flag == 'reload':
				self.reload_check_list()
				self.hc_flag = 'start'

			for addr in copy.deepcopy(self.check_list):
				jobs.append(self.check_arcus(addr))
				if len(jobs) > 10:
					await asyncio.wait(jobs) # python asyncio has overhead for huge list (as I tested)
					jobs = []

				if len(jobs) > 0:
					await asyncio.wait(jobs)
					jobs = []

			if self.state == 'l':
				if self.get_pending_time() >= 5: # if pending time is more than 5. this can be a splitted master
					self.log_error('It can be a splitted brain. wait a new leader')
					continue

				self.do_make_decision()

			# print working report every 60s
			if time.time() - last_printed_ts > 60:
				self.log_debug('repeated_fail_map: %s' % str(self.repeated_fail_map))
				last_printed_ts = time.time()
				
	def health_checker_thread(self):
		self.log_info('health check thread start')

		loop = asyncio.new_event_loop()
		loop.run_until_complete(self.check_all())

		self.log_info('health check thread is terminated')

	def watch_children(self, event):
		self.reload_check_list()

	def reload_check_list(self):
		children = self.zk.get_children('/arcus/cache_list/%s' % self.service_code, watch=self.watch_children)
		self.log_info('reload cache_list: %s' % str(children))
		tmp_check_list = []
		for child in children:
			tmp_check_list.append(child)

		self.check_list = tmp_check_list

	def on_start(self):
		self.zk = KazooClient(hosts=self.zk_addr)
		self.zk.start()

		self.hc_flag = 'start'
		self.reload_check_list()

		if not hasattr(node, 'checker_thread'):
			node.checker_thread = threading.Thread(target=self.health_checker_thread)
			node.checker_thread.start()

	def on_shutdown(self):
		self.zk.stop()

	def do_control(self, node, words):
		#print(words)

		if words[1] == 'hc':
			if len(words) <= 2:
				raise Exception("insufficient parameter")

			if words[2] in ['start', 'stop', 'reload']:
				node.hc_flag = words[2]
			else:
				raise Exception('invalid sub command')
		else:
			raise Exception('invalid sub command')


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-zk', dest='zk_addr', required=True, help='zookeeper address')
	parser.add_argument('-cloud', dest='cloud', required=True, help='cloud (service code) name')

	args = raft.parse_default_args(parser)

	node = ArcusMonitor(args.nid, args.addr, args.ensemble_map, args.zk_addr, args.cloud)
	node.worker.handler['control'] = [node.do_control, 'we', 1, -1]
	node.start()
	node.join()

