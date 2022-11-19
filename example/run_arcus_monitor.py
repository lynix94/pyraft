#!/usr/bin/env python

import os, sys, time, copy, re
import threading, argparse
import asyncio

from pyraft.common import RaftException
from pyraft import raft
from kazoo.client import KazooClient

class ArcusMonitor(raft.RaftNode):
	def __init__(self, nid, addr, ensemble, zk_addr, service_code, overwrite_peer):
		super().__init__(nid, addr, ensemble, overwrite_peer=overwrite_peer)

		self.zk_addr = zk_addr
		self.re_service_code = re.compile(service_code)
		self.repeated_fail_map = {}
		self.check_list_map = {}

		self.failover_threshold = 5
		self.failover_count = 0
		self.failover_count_limit = 10

		self.cooldown_time = 0
		self.cooldown_start = 0

	def do_failover(self, addr):
		self.log_info('process failover: %s' % addr)

		if self.cooldown_start > 0:
			if (time.time() - self.cooldown_start) > self.cooldown_time: # cooldown end
				self.log_info('cooldown time is over')
				self.failover_count = 0
				self.cooldown_start = 0
			else:
				self.log_info('skip failover because of cooldown time')
				return

		if self.failover_count > self.failover_count_limit: # start cooldown
			self.log_info('failover count exceeds limit %d. start cooldown time' % self.failover_count_limit)
			self.cooldown_start = time.time()
			return

		try:
			service_code = self.check_list_map[addr]
			self.log_info('delete /arcus/cache_list/%s/%s' % (service_code, addr))
			self.zk.delete('/arcus/cache_list/%s/%s' % (service_code, addr))
			self.failover_count += 1

		except Exception as e:
			self.log_info('node %s is already deleted (maybe gracefully shutdown): %s' % (addr, str(e)))

	def do_failback(self, addr):
		# nothing to do in ArcusMonitor
		pass

	def do_make_decision(self):
		for addr in self.check_list_map.keys():
			result = self.request('hgetall', 'hc_%s' % addr)
			opinions = {}
			for i in range(0, len(result), 2): # k, v parsing
				nid = result[i]
				repeated_fail = result[i+1]
				opinions[nid] = repeated_fail

			voted = 0.0
			for nid, repeated_fail in opinions.items():
				if repeated_fail >= self.failover_threshold:
					voted += 1.0

			if voted / len(opinions) > 0.5:
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

			for addr in self.check_list_map.keys():
				jobs.append(self.check_arcus(addr))
				if len(jobs) > 10:
					await asyncio.wait(jobs) # python asyncio has overhead for huge list (as I tested)
					jobs = []

			if len(jobs) > 0:
				await asyncio.wait(jobs)
				jobs = []

			if self.state == 'l':
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
		cloud_list = []
		service_codes = self.zk.get_children('/arcus/cache_list/')
		for service_code in service_codes:
			if self.re_service_code.match(service_code):
				cloud_list.append(service_code)

		tmp_check_list_map = {}
		for cloud in cloud_list:
			children = self.zk.get_children('/arcus/cache_list/%s' % cloud, watch=self.watch_children)
			self.log_info('reload %s cache_list: %s' % (cloud, str(children)))
			for child in children:
				tmp_check_list_map[child] = cloud

			self.check_list_map = tmp_check_list_map

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
				raise RaftException("insufficient parameter")

			if words[2] in ['start', 'stop', 'reload']:
				node.hc_flag = words[2]
			else:
				raise RaftException('invalid sub command')
		else:
			raise RaftException('invalid sub command')


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-zk', dest='zk_addr', required=True, help='zookeeper address')
	parser.add_argument('-cloud', dest='cloud', required=True, help='regex clouds (service code) name')
	parser.add_argument('-threshold', dest='failover_threshold', default=5, help='failover threshold (default 5)')
	parser.add_argument('-limit', dest='failover_count_limit', default=0, help='failover count limit (default 0 unlimited)')
	parser.add_argument('-cooldown', dest='cooldown_time', default=0, help='failover limit cool down time (default 0)')

	args = raft.parse_default_args(parser)

	node = ArcusMonitor(args.nid, args.addr, args.ensemble_map, args.zk_addr, args.cloud, args.overwrite_peer)
	node.failover_count_limit = args.failover_count_limit
	node.failover_threshold = args.failover_threshold
	node.cooldown_time = args.cooldown_time

	node.worker.handler['control'] = [node.do_control, 'we', 1, -1]
	node.start()
	node.join()
