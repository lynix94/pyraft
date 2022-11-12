#!/usr/bin/env python

from pyraft import raft

node = raft.make_default_node()

node.start()
node.join()



