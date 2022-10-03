import socket, select

class base_io:
    def __init__(self, sock):
        self.sock = sock
        self.buff = ''
        self.timeout = -1

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
            ret = self.sock.send(msg.encode())
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
            ret = self.sock.send(self.encode(msg).encode())
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
            if not self.decodable(self.buff):
                if timeout != None:
                    reads, writes, excepts = select.select([self.sock], [], [], timeout)
                    if len(reads) == 0:
                        return ''

                try:
                    tmp = self.sock.recv(4096).decode('utf-8')
                except socket.error:
                    self.close()
                    return None
                except Exception:
                    self.close()
                    return None

                if tmp == '':
                    self.close()
                    return None

                self.buff += tmp

            result, self.buff = self.decode(self.buff)
            if result == None:
                if timeout != None:
                    reads, writes, excepts = select.select([self.sock], [], [], timeout)
                    if len(reads) == 0:
                        return ''

                try:
                    tmp = self.sock.recv(4096).decode('utf-8')
                except socket.error:
                    self.close()
                    return None

                if tmp == '':
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

            if ret == '':
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

