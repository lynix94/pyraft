import socket
import select


def encoding(msg):
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
			result.append(encoding(item))  

		return ''.join(result)
			
	if isinstance(msg, Exception):
		return '-%s\r\n' % str(msg)

	return '-unknown resp type\r\n'

def decoding(src):
	#print 'decoding >>>> "%s"' % src
	if len(src) == 0:
		return None, src

	if src[0] == '*':
		toks = src.split('\r\n', 1)
		count = int(toks[0][1:])
		remain = toks[1]

		result = []
		for i in range(count):
			item, remain = decoding(remain)
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




class resp_io:
	def __init__(self, sock):
		self.sock = sock
		self.buff = ''
		self.timeout = -1

	def connected(self):
		return self.sock != None

	def write(self, msg, inline=False):
		if self.sock == None:
			return None

		if inline:
			if not msg.endswith('\r\n'):
				msg += '\r\n'

			try:
				ret = self.sock.send(msg.encode())
			except socket.error:
				self.sock = None
				return None

			if ret == 0:
				self.sock = None
				return None

			return ret

		try:
			ret =  self.sock.send(encoding(msg).encode())
			if ret == 0:
				self.sock = None
				return None
			return ret
				
		except socket.error:
			self.sock = None
			return None


	def read(self, timeout = None, split=False):
		if self.sock == None:
			return None

		while True:
			if self.buff.find('\r\n') < 0:
				if timeout != None:
					reads, writes, excepts = select.select([self.sock], [], [], timeout)
					if len(reads) == 0:
						return ''

				try:
					tmp = self.sock.recv(4096).decode('utf-8')
				except socket.error:
					self.sock = None
					return None

				if tmp == '':
					self.sock = None
					return None
				
				self.buff += tmp

			result, self.buff = decoding(self.buff)
			if result == None:
				if timeout != None:
					reads, writes, excepts = select.select([self.sock], [], [], timeout)
					if len(reads) == 0:
						return ''

				try:
					tmp = self.sock.recv(4096).decode('utf-8')
				except socket.error:
					self.sock = None
					return None

				if tmp == '':
					self.sock = None
					return None
				
				self.buff += tmp
				continue

			if split == True and isinstance(result, str):
				result = result.strip().split(' ')

			return result


	def read_all(self, timeout=None, split=False):
		result = []

		while True:
			ret = self.read(timeout, split)
			if ret == None:
				return None

			if ret == '':
				break

			result.append(ret)
			item, remain = decoding(self.buff)
			if item == None:
				break

		return result


	def close(self):
		if self.sock != None:
			self.sock.close()

		self.sock = None








