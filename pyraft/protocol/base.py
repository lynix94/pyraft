import socket, select

from pyraft.common import *

class base_io:
    def __init__(self, sock):
        self.sock = sock
        self.buff = b''
        self.timeout = -1

        self.last_decodable = False
        self.last_buff_len = 0

    def connected(self):
        return self.sock != None

    def close(self):
        if self.sock != None:
            self.sock.close()

        self.sock = None

    def raw_write(self, msg):
        if self.sock == None:
            return None

        msg = self.raw_encode(msg)

        try:
            if isinstance(msg, str):
                msg = msg.encode()
            ret = self.sock.send(msg)
        except socket.error:
            self.close()
            return None

        if ret == 0:
            self.close()
            return None

        return ret

    def write(self, msg):
        if self.sock == None:
            return None

        try:
            msg = self.encode(msg)
            if isinstance(msg, str):
                msg = msg.encode()

            ret = self.sock.send(msg)
            if ret == 0:
                self.close()
                return None
            return ret

        except socket.error:
            self.close()
            return None

    def read(self, timeout=None):
        if self.sock == None:
            return None

        while True:
            readable = False
            if timeout != None: # avoid useless decodable check
                if self.last_decodable == False and self.last_buff_len == len(self.buff):
                    reads, writes, excepts = select.select([self.sock], [], [], timeout)
                    if len(reads) == 0:
                        return b''
                    else:
                        readable = True

            self.last_decodable = True
            if not self.decodable(self.buff):
                if timeout != None and not readable: # skip double wait
                    reads, writes, excepts = select.select([self.sock], [], [], timeout)
                    if len(reads) == 0:
                        self.last_decodable = False
                        self.last_buff_len = len(self.buff)
                        return b''

                try:
                    tmp = self.sock.recv(4096)
                except socket.error:
                    self.close()
                    return None
                except Exception:
                    self.close()
                    return None

                if tmp == b'':
                    self.close()
                    return None

                self.buff += tmp

            result, self.buff = self.decode(self.buff)
            if result == None:
                if timeout != None:
                    reads, writes, excepts = select.select([self.sock], [], [], timeout)
                    if len(reads) == 0:
                        self.last_decodable = False
                        self.last_buff_len = len(self.buff)
                        return b''

                try:
                    tmp = self.sock.recv(4096)
                except socket.error:
                    self.close()
                    return None

                if tmp == b'':
                    self.close()
                    return None

                self.buff += tmp
                continue

            return result

    def read_all(self, timeout=None):
        result = []

        while True:
            ret = self.read(timeout)
            if ret == None:
                return None

            if ret == b'':
                break

            result.append(ret)
            item, remain = self.decode(self.buff)
            if item == None:
                break

        return result

    # inherit below 4 interface
    def raw_encode(self, msg):
        pass

    def encode(self, msg):
        pass

    def decode(self, msg):
        pass

    def decodable(self, buff):
        pass

