import threading, queue

from pyraft.common import *
from pyraft.protocol import resp

class LogItem(object):
    def __init__(self, term, index, ts, worker_offset, cmd):
        self.term = term
        self.index = index
        self.ts = ts
        self.cmd = cmd
        self.worker_offset = worker_offset

    def to_list(self):
        return [self.term, self.index, self.ts, self.worker_offset, self.cmd]

    def __repr__(self):
        return repr(self.to_list())


class LogFile(object):
    def __init__(self, nid, seq):
        self.seq = seq
        self.fn = 'raft_%s_%010d.log' % (nid, seq)
        self.fh = open(self.fn, 'w')
        self.count = 0
        self.start = 0
        self.end = 0

    def close(self):
        self.fh.close()

    def delete(self):
        self.fh.close()
        os.remove(self.fn)

    def append(self, item):
        self.fh.write(resp.resp_encoding(item.to_list()))
        self.fh.flush()
        self.count += 1

        if self.count == 0:
            self.start = item.index

        self.end = item.index

    def get_range(self, start, end=-1):
        decoded = []
        fh = open(self.fn, 'r')
        remain = fh.read()

        while True:
            l, remain = resp.resp_decoding(remain)
            if l == None:
                break

            decoded.append(LogItem(l[0], l[1], l[2], l[3], l[4]))

            if remain == '':
                break

        result = []
        for item in decoded:
            if item.index >= start:
                if end < 0 or item.index < end:
                    result.append(item)

        return result

    def start_index(self):
        return self.start

    def end_index(self):
        return self.end

    def size(self):
        return self.count


class LogFileStorage(object):
    def __init__(self, nid):
        self.index = 0
        self.file_list = []
        self.log = []
        self.nid = nid

    def append(self, item):
        self.index = item.index

        if len(self.file_list) == 0:
            lf = LogFile(self.nid, item.index)
            self.file_list.append(lf)
        else:
            count = self.file_list[-1].size() + 1
            if count > CONF_LOG_FILE_MAX:
                lf = LogFile(self.nid, item.index)
                self.file_list.append(lf)

        lf = self.file_list[-1]
        lf.append(item)

    def get_range(self, start, end=-1):
        result = []
        for item in self.file_list:
            if item.end_index() < start:
                continue

            if end > 0 and item.start_index() > end:
                break

            result += item.get_range(start, end)

        return result

    def cleanup(self, index):
        while len(self.file_list) > 0:
            f = self.file_list[0]
            if f.start_index() > index:
                break

            if f.end_index() <= index:
                f.delete()
                self.file_list.pop(0)
                continue

            break

    def start_index(self):
        if len(self.file_list) == 0:
            return self.index + 1

        return (self.file_list[0]).start_index()

    def size(self):
        count = 0

        for item in self.file_list:
            count += item.size()

        return count

    def close(self):
        for lf in self.file_list:
            lf.close()


class RaftLog(object):
    def __init__(self, nid):
        self.term = 0
        self.index = 0
        self.lock = threading.Lock()
        self.q = queue.Queue(4096)
        self.nid = nid
        self.log = LogFileStorage(self.nid)

        self.temp_item = []  # wait commit_index

    def apply_commit_index(self, commit_index):
        with self.lock:
            while len(self.temp_item) > 0:
                item = self.temp_item[0]
                if item == None:
                    del self.temp_item[0]
                    continue

                if item.index <= commit_index:
                    self._q_push(item)
                    last_index = item.index
                    del self.temp_item[0]
                else:
                    break

    def _q_push(self, item):
        self.q.put(item)

        if isinstance(item.cmd, Future):
            new_item = LogItem(item.term, item.index, item.ts, item.worker_offset, item.cmd.cmd)
            self.log.append(new_item)
        else:
            self.log.append(item)

    def push(self, item, commit_index):
        self.term = item.term
        self.index = item.index

        if item.index > commit_index:  # wait until commit
            with self.lock:
                for i in range(len(self.temp_item)):  # clean up invalid item
                    if self.temp_item[i].index >= item.index:
                        self.temp_item[i] = None

                self.temp_item.append(item)
            return

        with self.lock:
            self._q_push(item)

    def pop(self, timeout):
        try:
            item = self.q.get(True, timeout)
        except queue.Empty:
            return None

        return item

    def get_range(self, start, end=-1):
        with self.lock:
            return self.log.get_range(start, end)

    def cleanup(self, index):
        with self.lock:
            return self.log.cleanup(index)

    def start_index(self):
        with self.lock:
            return self.log.start_index()

    def size(self):
        with self.lock:
            return self.log.size()

    def get_index(self):
        return self.index

    def get_term(self):
        return self.term

    def close(self):
        self.log.close()



