#!/usr/bin/env python

import os, sys, time, copy
import threading
import asyncio
import aioping

from pyraft import raft


import logging


class PingMonitor(raft.RaftNode):
	def __init__(self, nid, addr, ensemble):
		super().__init__(nid, addr, ensemble)

		self.repeated_fail_map = {}
		self.failover_threshold = 5

		self.failed_list_map = {}

	def do_failover(self, ip):
		self.log_info('process failover: %s' % ip)
		self.faild_list_map[ip] = True
		# notify syslog

	def do_failback(self, ip):
		del(self.failed_list_map[ip])
		# notify syslog enable message

	def do_make_decision(self):
		check_list = copy.deepcopy(self.data['check_list'])

		for ip in check_list:
			result = self.request('hgetall', 'hc_%s' % ip)
			opinions = {}
			for i in range(0, len(result), 2):  # k, v parsing
				nid = result[i]
				repeated_fail = result[i + 1]
				opinions[nid] = repeated_fail

			voted = 0.0
			for nid, repeated_fail in opinions.items():
				if repeated_fail >= self.failover_threshold:
					voted += 1.0

			if voted / len(opinions) > 0.5:
				self.do_failover(ip)
			else:
				if ip in self.failed_list_map:
					self.do_failback(ip)

	async def check_ping(self, ip):
		print('check ping: ', ip)
		if ip not in self.repeated_fail_map:
			self.repeated_fail_map[ip] = 0
			self.request('hset', 'hc_%s' % (ip), self.nid, 0)

		repeated_fail = self.repeated_fail_map[ip]

		try:
			delay = await aioping.ping(ip, timeout=1.0) * 1000
			print ('ping delay %s ms' % delay)
			
		except Exception as e:
			self.log_error('ping to %s failed: %s' % (ip, str(e)))
			self.repeated_fail_map[ip] = repeated_fail + 1
			self.request('hset', 'hc_%s' % (ip), self.nid, repeated_fail + 1)
			return

		# in success
		if self.repeated_fail_map[ip] != 0:  # reset if needed only (efficiency)
			self.repeated_fail_map[ip] = 0
			self.request('hset', 'hc_%s' % (ip), self.nid, 0)

	async def check_all(self):
		last_printed_ts = 0
		while not self.shutdown_flag:
			time.sleep(1.0)

			if 'check_list' not in self.data:
				continue

			jobs = []
			check_list = copy.deepcopy(self.data['check_list'])

			for ip in check_list:
				jobs.append(self.check_ping(ip))
				if len(jobs) > 10:
					await asyncio.wait(jobs)  # python asyncio has overhead for huge list (as I tested)
					jobs = []

				if len(jobs) > 0:
					await asyncio.wait(jobs)
					jobs = []

			if self.state == 'l':
				self.do_make_decision()

			# print working report every 60s
			if time.time() - last_printed_ts > 60:
				self.log_info('repeated_fail_map: %s' % str(self.repeated_fail_map))
				last_printed_ts = time.time()

	def health_checker_thread(self):
		self.log_info('health check thread start')

		loop = asyncio.new_event_loop()
		loop.run_until_complete(self.check_all())

		self.log_info('health check thread is terminated')

	def on_start(self):
		self.data['check_list'] = ['127.0.0.2', '127.0.0.1']
		if not hasattr(node, 'checker_thread'):
			node.checker_thread = threading.Thread(target=self.health_checker_thread)
			node.checker_thread.start()


logger = logging.getLogger('pyraft')
logger.setLevel(logging.DEBUG)

node = PingMonitor('1', '127.0.0.1:5010', {})
node.start()
node.join()

