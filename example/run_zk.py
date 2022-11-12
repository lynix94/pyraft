#!/usr/bin/env python

import os, sys, time
import argparse

from pyraft import raft
from pyraft.worker.zk_worker import ZkWorker

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	args = raft.parse_default_args(parser)

	node = raft.RaftNode(args.nid, args.addr, args.ensemble_map)
	if args.load != None:
		node.load(args.load)

	ip, port = args.addr.split(':')
	zk = ZkWorker('%s:%d' % (ip, int(port)+2)) # +1 is used for raft protocol
	node.regist_worker(2, zk)
	node.start()
	node.join()
