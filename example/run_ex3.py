#!/usr/bin/env python

import os, sys, time
import threading
from pyraft import raft


def do_cas(node, words):
	key = words[1]
	expected = words[2]
	value = words[3]

	node.check_ttl(key) # lazy deleting which expired

	if key not in node.data:
		if expected.lower() == 'none':
			node.data[key] = expected
		else:
			return None

	old_value = node.data[key]
	if old_value != expected:
		return old_value

	node.data[key] = value
	node.clear_ttl(key)
	return expected


node = raft.make_default_node()

node.worker.handler['cas'] = [do_cas, 'we', 3, 3] # [function, type(we means write & entry), minimum param, maximum param]

node.start()
node.join()



