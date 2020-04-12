import os, sys, time
import threading
from pyraft import raft

node = raft.make_default_node()

node.start()
node.join()



