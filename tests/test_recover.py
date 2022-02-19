import unittest

import os, sys, time
import threading
import test_util as test

from pyraft import raft


class TestRecover(unittest.TestCase):
	def test_recover(self):
		nodes = []

		n = test.make_test_node(1, nodes) # 1 is leader
		n.start()
		nodes.append(n)
		assert test.check_state(nodes)
		assert test.set_test_value(nodes)
		assert test.check_test_value(nodes)

		n = test.make_test_node(2, nodes)
		n.start()
		nodes.append(n)
		assert test.check_state(nodes)
		assert test.check_test_value(nodes)
		assert test.set_test_value(nodes)
		assert test.check_test_value(nodes)


		n = test.make_test_node(3, nodes)
		n.start()
		nodes.append(n)
		assert test.check_state(nodes)
		assert test.check_test_value(nodes)
		assert test.set_test_value(nodes)
		assert test.check_test_value(nodes)

		n = nodes[0]
		nodes = nodes[1:]

		n.shutdown()
		n.join()

		assert test.check_state(nodes) # should one of 2, 3 should be leader
		assert test.check_test_value(nodes)
		assert test.set_test_value(nodes)
		assert test.check_test_value(nodes)


		n = test.make_test_node(1, nodes)
		n.start()
		nodes.append(n)
		assert test.check_state(nodes) # now 1 is joined as follower


		for n in nodes:
			n.shutdown()
			n.join()

if __name__ == '__main__':
	unittest.main()


