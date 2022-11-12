#!/usr/bin/env python

import os, sys, time
import threading, urllib.request, urllib.error, urllib.parse
from pyraft import raft




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



