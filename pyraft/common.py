import os, sys, time
import socket, threading, logging

CONF_LOG_MAX = 100000
CONF_LOG_FILE_MAX = 10000

CONF_VOTING_TIME = 1.0
CONF_PING_TIMEOUT = 5  # re-elect leader after CONF_PING_TIMEOUT

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

class RaftException(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class Future(object):
    def __init__(self, cmd, worker_offset=0):
        self.worker_offset = worker_offset
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

def bytes_to_str(b):
    out = []
    for c in b:
        out.append('%02x' % c)

    return ' '.join(out)

logger = logging.getLogger('pyraft')