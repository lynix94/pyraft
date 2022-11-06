import traceback

from pyraft.common import *
from pyraft.protocol import resp


class RespProtocol:
    def open_io(self, handle):
        return resp.resp_io(handle)


class Worker:
    def __init__(self, addr):
        self.handler = {}
        self.shutdown_flag = False
        self.p = None

        self.addr = addr
        self.ip, self.port = addr.split(':')
        self.port = int(self.port)
        self.worker_offset = 0 # offset between node baseport

    def init_node(self, node):
        pass # inherit for worker specific node init

    def set_protocol(self, p):
        self.p = p

    def start(self, node):
        if self.p is None:
            self.p = RespProtocol()

        self.th_worker = threading.Thread(target=self.worker_listen, args=(node,))
        self.th_worker.start()

    def shutdown(self):
        self.shutdown_flag = True

    def join(self):
        self.th_worker.join()

    def worker_listen(self, node):
        self.worker_listen_sock = socket.socket()
        self.worker_listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.worker_listen_sock.bind((self.ip, self.port))
        self.worker_listen_sock.listen(1)
        self.worker_listen_sock.settimeout(1)

        while True:
            try:
                sock, addr = self.worker_listen_sock.accept()
                wt = threading.Thread(target=self.process_work, args=(node, sock,))
                wt.start()
            except socket.timeout:
                if self.shutdown_flag:
                    self.worker_listen_sock.close()
                    break

    def get_handler(self, name):
        if name not in self.handler:
            return None

        return self.handler[name]

    def process_work(self, node, sock):
        pio = self.p.open_io(sock)

        while True:
            if self.shutdown_flag:
                pio.close()
                return

            words = pio.read(timeout=1.0)
            if words == None:
                node.log_info('disconnected')
                pio.close()
                return

            if isinstance(words, str): # resp inline command
                words = words.split()

            if words == b'' or words == ['']:
                continue

            if len(words) > 0:
                handler = self.get_handler(words[0].lower())
                if handler == None:
                    pio.write(Exception('Unknown command: %s' % words[0]))
                    continue

                p_min = handler[2]
                p_max = handler[3]
                if len(words) - 1 < p_min:
                    pio.write(Exception('insufficient param'))
                    continue

                if p_max > 0 and len(words) - 1 > p_max:
                    pio.write(Exception('too many param'))
                    continue

                try:
                    ret = node.propose(words, self.worker_offset)
                except RaftException as e:
                    ret = e
                except Exception as e:
                    print('unexpected exception: ', traceback.format_exc())
                    ret = e

                # for special actions (like quit)
                if isinstance(ret, dict):
                    if 'bypass' in ret:
                        pio.raw_write(ret['bypass'])
                        continue

                    if 'quit' in ret:
                        pio.close()
                        return

                    # some protocols have to send something before closing
                    if 'quit_after_send' in ret:
                        pio.write(ret['quit_after_send'])
                        pio.close()
                        return

                pio.write(ret)

    def relay_cmd(self, leader, cmd, worker_offset):
        p = leader
        try:
            if not hasattr(p, 'req_io'):
                ip, port = p.addr.split(':')
                port = int(port) + worker_offset
                sock = socket.socket()
                sock.connect((ip, port))
                p.req_io = self.p.open_io(sock)

            p.req_io.write(cmd)
            return p.req_io.read()

        except Exception as e:
            p.req_io.close()
            delattr(p, 'req_io')
            raise RaftException('relay to leader has exception: %s' % str(e))


class MergedWorker(Worker):
    def __init__(self, addr, *workers):
        super().__init__(addr)
        self.worker_list = workers

    def get_handler(self, name):
        if name in self.handler:
            return self.handler[name]

        for worker in self.worker_list:
            handler = worker.get_handler(name)
            if handler is not None:
                return handler

        return None

    def append_worker(self, worker):
        self.worker_list.append(worker)
