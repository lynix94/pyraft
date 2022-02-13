import sys, threading

import select
import socket

CONF_LOG_MAX = 100000
CONF_LOG_FILE_MAX = 10000

CONF_VOTING_TIME = 1.0
CONF_PING_TIMEOUT = 5  # re-elect leader after CONF_PING_TIMEOUT

g_log_level = 0
g_log_handle = sys.stdout

def log_write(log):
    global g_log_handle
    g_log_handle.write(log)

def set_log_level(level):  # debug, info, warn, error
    global g_log_level

    if level.lower().startswith('debug'):
        g_log_level = 0
    elif level.lower().startswith('info'):
        g_log_level = 1
    elif level.lower().startswith('warn'):
        g_log_level = 2
    elif level.lower().startswith('err'):
        g_log_level = 3

def get_log_level():
    global g_log_level
    return g_log_level

def intcast(src):
    if isinstance(src, int):
        return src

    if src.isdigit() == False:
        return None

    return int(src)


ERROR_CAST = Exception('number format error')
ERROR_APPEND_ENTRY = Exception('append entry failed')
ERROR_TYPE = Exception('invalid data type')
ERROR_NOT_EXISTS = Exception('not exists')
ERROR_INVALID_PARAM = Exception('invalid parameter')


class Future(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.value = None
        self.cond = threading.Condition()

    def get(self, timeout=None):
        if self.value != None:
            return self.value

        try:
            with self.cond:
                self.cond.wait(timeout)
        except RuntimeError:
            return None

        return self.value

    def set(self, value):
        with self.cond:
            self.value = value
            self.cond.notify()

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

