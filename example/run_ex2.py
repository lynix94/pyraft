#!/usr/bin/env python

import os, sys, time
import threading
from pyraft import raft


def session_checker(node):
	while not node.shutdown_flag:
		time.sleep(5)

		if node.state != 'l':
			continue

		for k, v in list(node.data.items()):
			if not k.startswith('session_'):
				continue

			ts = int(v)
			curr_ts = time.time()

			if curr_ts - ts < 60:
				print('session %s is ok (diff: %d)' % (curr_ts - ts))
			else:
				print('session %s is EXPIRED (diff: %d)' % (curr_ts - ts))


def session_check_start(node):
	if not hasattr(node, 'cheker'):
		node.checker = threading.Thread(target=session_checker, args=(node,))
		node.checker.start()
	

node = raft.make_default_node()

node.worker.handler['on_leader'] = session_check_start

node.start()
node.join()



