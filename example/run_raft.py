import os, sys, time
import threading
from pyraft import raft
import logging

logger = logging.getLogger('pyraft')
logger.setLevel(logging.DEBUG)

node = raft.make_default_node()

node.start()
node.join()



