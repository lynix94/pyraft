import unittest

import os, sys, time
import threading
import test_util as test

from pyraft import raft


class TestVote(unittest.TestCase):
	def normal_election(self, n_node_list, repeat):
		i = 0
		while i < repeat:
			for n_node in n_node_list:
				i += 1

				nodes, ensemble = test.make_test_node_set(n_node)

				for n in nodes:
					n.start()

				ret = test.check_state(nodes, timeout = 10 + 2*len(nodes))

				for n in nodes:
					n.shutdown()
					n.join()

				if ret == False:
					return False

		return True


	def test_election(self):
		assert self.normal_election([3,3,3,3,3,4,5,5,5,5,5,6,7,7,7,7,7,8,9,10,11,12,13], 100)

if __name__ == '__main__':
	unittest.main()


