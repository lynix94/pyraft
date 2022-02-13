
from pyraft.common import *
from pyraft.protocol.base import base_io

def resp_encoding(msg):
	if isinstance(msg, bool) and msg == True:
		return '+OK\r\n'

	if msg == None:
		return '$-1\r\n'

	if isinstance(msg, str):
		if len(msg) < 10:
			return '+%s\r\n' % msg
		else:
			return '$%d\r\n%s\r\n' % (len(msg), msg)

	if isinstance(msg, int):
		return ':%d\r\n' % msg

	if isinstance(msg, float): 
		return '+%s\r\n' % str(msg)

	if isinstance(msg, list) or isinstance(msg, tuple):
		result = ['*%d\r\n' % len(msg)]
		for item in msg:
			result.append(resp_encoding(item))

		return ''.join(result)
			
	if isinstance(msg, Exception):
		return '-%s\r\n' % str(msg)

	return '-unknown resp type\r\n'

def resp_decoding(src):
	#print 'decoding >>>> "%s"' % src
	if len(src) == 0:
		return None, src

	if src[0] == '*':
		toks = src.split('\r\n', 1)
		count = int(toks[0][1:])
		remain = toks[1]

		result = []
		for i in range(count):
			item, remain = resp_decoding(remain)
			if item == None:
				return None, src

			result.append(item)

		return result, remain

	if src[0] == '$':
		toks = src.split('\r\n', 1)
		if len(toks) != 2:
			return None, src

		size = int(toks[0][1:])
		remain = toks[1]
		if len(remain) < size+2:
			return None, src

		return remain[:size], remain[size+2:]
		
	if src[0] == '+':
		toks = src.split('\r\n', 1)
		if len(toks) != 2:
			return None, src

		return toks[0][1:], toks[1]

	if src[0] == '-':
		toks = src.split('\r\n', 1)
		if len(toks) != 2:
			return None, src

		return Exception(toks[0][1:]), toks[1]

	if src[0] == ':':
		toks = src.split('\r\n', 1)
		if len(toks) != 2:
			return None, src

		return int(toks[0][1:]), toks[1]

	# plain text input
	toks = src.split('\r\n', 1)
	if len(toks) == 1:
		return src, ''

	return toks[0], toks[1]


class resp_io(base_io):
	def __init__(self, sock):
		super(resp_io, self).__init__(sock)

	def raw_encode(self, msg):
		if not msg.endswith('\r\n'):
			msg += '\r\n'

		return msg

	def encode(self, msg):
		return resp_encoding(msg)

	def decode(self, msg):
		return resp_decoding(msg)

	def decodable(self, buff):
		if '\r\n' in buff:
			return True

		return False
