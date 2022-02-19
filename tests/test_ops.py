import unittest

import os, sys, time
import threading
import test_util as test
import redis, random

from pyraft import raft

class TestOPS(unittest.TestCase):
	def test_ops(self):
		nodes = []

		n1 = test.make_test_node(1, nodes) # 1 is leader
		n1.start()

		count = 1000
		time.sleep(1)

		r = redis.StrictRedis(host=n1.ip, port=n1.port)

		start = time.time()
		for i in range(count):
			v = 'value_%07d' % random.randrange(0, 1000000)
			r.set('key_%d' % i, v)
		end = time.time()
		print('ops: %d' % (count / (end-start)))


		start = time.time()
		for i in range(count):
			v = r.get('get_%d' % i)
		end = time.time()
		print('ops: %d' % (count / (end-start)))



		n2 = test.make_test_node(2, nodes)
		n2.start()

		start = time.time()
		for i in range(count):
			v = 'value_%07d' % random.randrange(0, 1000000)
			r.set('key_%d' % i, v)
		end = time.time()
		print('ops: %d' % (count / (end-start)))


		start = time.time()
		for i in range(count):
			v = r.get('get_%d' % i)
		end = time.time()
		print('ops: %d' % (count / (end-start)))


		n3 = test.make_test_node(3, nodes)
		n3.start()

		start = time.time()
		for i in range(count):
			v = 'value_%07d' % random.randrange(0, 1000000)
			r.set('key_%d' % i, v)
		end = time.time()
		print('ops: %d' % (count / (end-start)))


		start = time.time()
		for i in range(count):
			v = r.get('get_%d' % i)
		end = time.time()
		print('ops: %d' % (count / (end-start)))

		n1.shutdown()
		n1.join()
		n2.shutdown()
		n2.join()
		n3.shutdown()
		n3.join()

if __name__ == '__main__':
    unittest.main()



