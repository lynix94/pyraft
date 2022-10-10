# basic proxy tool for protocol analysis

import socket, threading, select
import argparse

def dump_str(data):
	result = []
	result_ascii = []
	for i in data:
		result.append('%02x ' % i)
		result_ascii.append('%c ' % i)

	return '%s - %s' % (''.join(result), ''.join(result_ascii))

def handle_client(server, c, addr):
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		ip, port = server.split(':')
		s.connect((ip, int(port)))

		while True:
			rl, wl, el = select.select([c], [], [])
			for r in rl:
				data = r.recv(4096)
				if not data:
					c.close()
					return

				print ('<< req[%d]: %s' % (len(data), dump_str(data)))
				s.sendall(data)

			rl, wl, el = select.select([s], [], [], 0.01)
			for r in rl:
				data = r.recv(4096)
				if not data:
					c.close()
					return

				print ('>> resp[%d]: %s' % (len(data), dump_str(data)))
				c.sendall(data)



def do_proxy(server, listen):
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	ip, port = listen.split(':')
	s.bind((ip, int(port)))
	s.listen(10)
	while True:
		conn, addr = s.accept()
		th = threading.Thread(target=handle_client, args=(server, conn, addr))
		th.start()

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-s', dest='server', help='server side address', required=True)
	parser.add_argument('-a', dest='addr', help='listen address', required=True)

	args = parser.parse_args()

	do_proxy(args.server, args.addr)

	
