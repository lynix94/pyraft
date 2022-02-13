import time, socket

from pyraft.common import *
from pyraft import resp

class RespProtocol(object):
    def open_io(self, handle):
        return resp.resp_io(handle)

class Worker(object):
    def __init__(self):
        self.handler = {}
        self.shutdown_flag = False
        self.p = None

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
        self.worker_listen_sock.bind((node.ip, node.port))
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

            if isinstance(words, str):
                words = words.split()

            if words == ['']:
                continue

            if len(words) > 0:
                if words[0].lower() not in self.handler:
                    pio.write(Exception('Unknown command: %s' % words[0]))
                    continue

                handler = self.handler[words[0].lower()]
                p_min = handler[2]
                p_max = handler[3]
                if len(words) - 1 < p_min:
                    pio.write(Exception('insufficient param'))
                    continue

                if p_max > 0 and len(words) - 1 > p_max:
                    pio.write(Exception('too many param'))
                    continue

                try:
                    ret = node.propose(words)
                except Exception as e:
                    ret = e

                if isinstance(ret, dict) and 'quit' in ret:
                    pio.close()
                    return

                pio.write(ret)

    def relay_cmd(self, leader, cmd):
        p = leader
        try:
            if not hasattr(p, 'req_io'):
                ip, port = p.addr.split(':')
                sock = socket.socket()
                sock.connect((ip, int(port)))
                p.req_io = self.p.open_io(sock)

            p.req_io.write(cmd)
            return p.req_io.read()

        except Exception as e:
            p.req_io.close()
            delattr(p, 'req_io')
            raise Exception('relay to leader has exception: %s', str(e))


class BaseWorker(Worker):
    def __init__(self):
        super(BaseWorker, self).__init__()
        self.init_base_handler()

    # inherit & extend this interface
    def init_base_handler(self):
        self.handler['info'] = [self.do_info, 'r', 0, 0]
        self.handler['shutdown'] = [self.do_shutdown, 'e', 0, 0]
        self.handler['quit'] = [self.do_quit, 'r', 0, 0]

        self.handler['add_node'] = [self.do_add_node, 'we', 2, 2]
        self.handler['del_node'] = [self.do_del_node, 'we', 1, 1]
        self.handler['checkpoint'] = [self.do_checkpoint, 'r', 0, 0]
        self.handler['getdump'] = [self.do_getdump, 'r', 0, 0]
        self.handler['getlog'] = [self.do_getlog, 'r', 0, 2]

    def do_info(self, node, words):
        peers = {}
        for nid, p in node.get_peers().items():
            peers[nid] = {'state': p.state, 'addr': p.addr, 'term': p.term, 'index': p.index}

        info = {'nid': node.nid, 'state': node.state, 'term': node.term, 'index': node.index,
                'peers': peers, 'last_checkpoint': node.last_checkpoint}
        return str(info).replace("'", '"')

    def do_quit(self, node, words):
        return {'quit': True}

    def do_shutdown(self, node, words):
        node.shutdown()
        return True

    def do_add_node(self, node, words):
        node.add_node(words[1], words[2])
        return True

    def do_del_node(self, node, words):
        node.del_node(words[1])
        return True

    def do_checkpoint(self, node, words):
        name = None
        if len(words) > 1:
            name = words[1]

        node.checkpoint(name)
        return True

    def do_getdump(self, node, words):
        return node.get_snapshot()

    def do_getlog(self, node, words):
        start = 0
        end = -1

        if len(words) > 1:
            start = intcast(words[1])
            if start == None:
                raise ERROR_CAST

        if len(words) > 2:
            end = intcast(words[2])
            if end == None:
                raise ERROR_CAST

        result = node.log.get_range(start, end)
        return result.__repr__()


class RaftWorker(BaseWorker):
    def __init__(self):
        super(RaftWorker, self).__init__()
        self.init_raft_handler()

    def init_raft_handler(self):
        # String
        self.handler['get'] = [self.do_get, 'r', 1, 1]
        self.handler['del'] = [self.do_del, 'we', 1, 1]
        self.handler['set'] = [self.do_set, 'we', 2, 2]
        self.handler['expire'] = [self.do_expire, 'we', 2, 2]
        self.handler['expireat'] = [self.do_expireat, 'we', 2, 2]
        self.handler['pexpire'] = [self.do_pexpire, 'we', 2, 2]
        self.handler['pexpireat'] = [self.do_pexpireat, 'we', 2, 2]

        # Hash
        self.handler['hgetall'] = [self.do_hgetall, 'r', 1, 1]
        self.handler['hset'] = [self.do_hset, 'we', 3, -1]
        self.handler['hget'] = [self.do_hget, 'r', 2, 2]
        self.handler['hdel'] = [self.do_hdel, 'we', 2, -1]
        self.handler['hlen'] = [self.do_hlen, 'r', 1, 1]

        # List
        self.handler['lpush'] = [self.do_lpush, 'we', 2, -1]
        self.handler['rpush'] = [self.do_rpush, 'we', 2, -1]
        self.handler['rpop'] = [self.do_rpop, 'we', 1, 1]
        self.handler['lpop'] = [self.do_lpop, 'we', 1, 1]
        self.handler['lrange'] = [self.do_lrange, 'r', 3, 3]
        self.handler['lindex'] = [self.do_lindex, 'r', 2, 2]
        self.handler['llen'] = [self.do_llen, 'r', 1, 1]
        self.handler['lset'] = [self.do_lset, 'we', 3, 3]
        self.handler['lrem'] = [self.do_lrem, 'we', 3, 3]
        self.handler['ltrim'] = [self.do_ltrim, 'we', 3, 3]

    def do_lpush(self, node, words):
        key = words[1].strip()
        node.check_ttl(key)

        values = words[2:]

        if key not in node.data:
            node.data[key] = []

        lobj = node.data[key]
        if not isinstance(lobj, list):
            raise ERROR_TYPE

        for v in values:
            lobj.insert(0, v)

        return len(lobj)


    def do_rpush(self, node, words):
        key = words[1].strip()
        node.check_ttl(key)

        values = words[2:]

        if key not in node.data:
            node.data[key] = []

        lobj = node.data[key]

        if not isinstance(lobj, list):
            raise ERROR_TYPE

        for v in values:
            lobj.append(v)

        return len(lobj)

    def do_rpop(self, node, words):
        key = words[1].strip()
        node.check_ttl(key)

        if key in node.data:
            lobj = node.data[key]

            if not isinstance(lobj, list):
                raise ERROR_TYPE

            return lobj.pop()

        return None

    def do_lpop(self, node, words):
        key = words[1].strip()
        node.check_ttl(key)

        if key in node.data:
            lobj = node.data[key]

            if not isinstance(lobj, list):
                raise ERROR_TYPE

            return lobj.pop(0)

        return None

    def do_lrange(self, node, words):
        key = words[1].strip()
        node.check_ttl(key)

        start = int(words[2])
        end = int(words[3]) + 1

        if key in node.data:
            lobj = node.data[key]

            if not isinstance(lobj, list):
                raise ERROR_TYPE

            if end == 0:
                end = len(lobj)

            return lobj[start:end]

        return None

    def do_lindex(self, node, words):
        key = words[1].strip()
        node.check_ttl(key)

        index = int(words[2])

        if key in node.data:
            lobj = node.data[key]
            if not isinstance(lobj, list):
                raise ERROR_TYPE

            return lobj[index]

        return None

    def do_llen(self, node, words):
        key = words[1].strip()
        node.check_ttl(key)

        if key in node.data:
            lobj = node.data[key]
            if not isinstance(lobj, list):
                raise ERROR_TYPE

            return len(lobj)

        return 0

    def do_lset(self, node, words):
        key = words[1].strip()
        node.check_ttl(key)

        index = int(words[2])
        value = words[3]

        if key in node.data:
            lobj = node.data[key]

            if not isinstance(lobj, list):
                raise ERROR_TYPE

            lobj[index] = value

        return True

    def do_lrem(self, node, words):
        key = words[1].strip()
        node.check_ttl(key)

        count = int(words[2])
        value = words[3]

        if key in node.data:
            lobj = node.data[key]

            if not isinstance(lobj, list):
                raise ERROR_TYPE

            org_len = len(lobj)

            if count > 0:
                for i in range(count):
                    try:
                        lobj.remove(value)
                    except ValueError:
                        break

                return org_len - len(lobj)
            elif count == 0:
                l = [x for x in lobj if x != value]
                node.data[key] = l
                return org_len - len(l)
            else:
                raise ERROR_INVALID_PARAM

        return 0

    def do_ltrim(self, node, words):
        key = words[1].strip()
        node.check_ttl(key)

        start = int(words[2])
        end = int(words[3]) + 1

        if key in node.data:
            lobj = node.data[key]

            if not isinstance(lobj, list):
                raise ERROR_TYPE

            if end == 0:
                end = len(lobj)
            ret = lobj[start:end]
            node.data[key] = lobj

        return True

    def do_hgetall(self, node, words):
        key = words[1].strip()
        node.check_ttl(key)

        result = []
        if key in node.data:
            hobj = node.data[key]

            if not isinstance(hobj, dict):
                raise ERROR_TYPE

            for k, v in hobj.items():
                result.append(k)
                result.append(v)

        return result

    def do_hdel(self, node, words):
        key = words[1].strip()
        node.check_ttl(key)

        count = 0
        if key in node.data:
            hobj = node.data[key]

            if not isinstance(hobj, dict):
                raise ERROR_TYPE

            if words[2] in hobj:
                del (hobj[words[2]])
                count += 1

        return count

    def do_hset(self, node, words):
        key = words[1].strip()
        node.check_ttl(key)

        if key not in node.data:
            node.data[key] = {}

        hobj = node.data[key]
        if not isinstance(hobj, dict):
            raise ERROR_TYPE

        hobj[words[2]] = words[3]

        return True

    def do_hlen(self, node, words):
        key = words[1].strip()
        node.check_ttl(key)

        if key in node.data:
            hobj = node.data[key]
            if not isinstance(hobj, dict):
                raise ERROR_TYPE

            return len(hobj)

        return 0

    def do_hget(self, node, words):
        key = words[1].strip()
        node.check_ttl(key)

        if key in node.data:
            hobj = node.data[key]

            if not isinstance(hobj, dict):
                raise ERROR_TYPE

            if words[2] not in hobj:
                raise ERROR_NOT_EXISTS

            return hobj[words[2]]

        return None

    def do_get(self, node, words):
        key = words[1].strip()
        node.check_ttl(key)

        if key in node.data:
            value = node.data[key]
            if not isinstance(value, str):
                raise ERROR_TYPE

            return value

        return None

    def do_del(self, node, words):
        key = words[1].strip()
        if key in node.data:
            del node.data[key]
            node.clear_ttl(key)
            return 1

        return 0

    def do_set(self, node, words):
        node.data[words[1]] = words[2]
        node.clear_ttl(words[1])
        return True

    # TODO: cleanup expired by backgroud thread
    def pexpireat(self, node, key, ts):
        ts = float(ts) / 1000.0
        return node.set_ttl(key, ts)

    def do_pexpireat(self, node, words):
        ts = intcast(words[2])
        if ts == None:
            raise ERROR_CAST

        return self.pexpreat(node, words[1], ts)

    def do_pexpire(self, node, words):
        msec = intcast(words[2])
        if msec == None:
            raise ERROR_CAST

        return self.pexpireat(node, words[1], int(time.time() * 1000) + msec)

    def do_expireat(self, node, words):
        ts = intcast(words[2])
        if ts == None:
            raise ERROR_CAST

        return self.pexpreat(node, words[1], ts * 1000)

    def do_expire(self, node, words):
        sec = intcast(words[2])
        if sec == None:
            raise ERROR_CAST

        return self.pexpireat(node, words[1], int(time.time() * 1000) + (sec * 1000))

