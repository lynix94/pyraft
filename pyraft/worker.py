import threading, socket

from pyraft.common import *
from pyraft import resp


class BaseWorker(object):
    def __init__(self):
        self.handler = {}
        self.init_handler()
        self.node = None

    def set_node(self, node):
        self.node = node

    def start(self):
        self.worker_listen_sock = socket.socket()
        self.th_worker = threading.Thread(target=self.worker_listen)
        self.th_worker.start()

        self.th_apply = threading.Thread(target=self.apply_loop)
        self.th_apply.start()

    def join(self):
        self.th_worker.join()
        self.th_apply.join()
        self.worker_listen_sock.close()

    def worker_listen(self):
        self.worker_listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.worker_listen_sock.bind((self.node.ip, self.node.port))
        self.worker_listen_sock.listen(1)

        self.worker_listen_sock.settimeout(1)
        while True:
            try:
                sock, addr = self.worker_listen_sock.accept()
                wt = threading.Thread(target=self.process_work, args=(sock,))
                wt.start()
            except socket.timeout:
                if self.node.shutdown_flag:
                    break
                continue

    # inherit & extend this interface
    def init_handler(self):
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
        for nid, p in self.node.get_peers().items():
            peers[nid] = {'state': p.state, 'addr': p.addr, 'term': p.term, 'index': p.index}

        info = {'nid': self.node.nid, 'state': self.node.state, 'term': self.node.term, 'index': self.node.index,
                'peers': peers, 'last_checkpoint': self.node.last_checkpoint}
        return str(info).replace("'", '"')

    def do_quit(self, node, words):
        return {'quit': True}

    def do_shutdown(self, node, words):
        self.node.shutdown_flag = True
        return True

    def do_add_node(self, node, words):
        self.node.add_node(words[1], words[2])
        return True

    def do_del_node(self, node, words):
        self.node.del_node(words[1])
        return True

    def do_checkpoint(self, node, words):
        name = None
        if len(words) > 1:
            name = words[1]

        self.node.checkpoint(name)
        return True

    def do_getdump(self, node, words):
        return self.node.get_snapshot()

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

        result = self.node.log.get_range(start, end)
        return result.__repr__()

    def propose(self, cmd):
        handler = self.handler[cmd[0].lower()]

        if 'e' in handler[1]:
            if self.node.state == 'c':
                self.node.log_warn('request while candidate')
                raise Exception('temporary unavailable')

            if self.node.state != 'l':
                raise Exception('recconect to leader')

            f = Future(cmd)
            self.node.q_entry.put(f)

            ret = f.get(10)
            if ret == ERROR_APPEND_ENTRY:
                self.node.log_info('append_entry failed (%s)' % str(words))
        else:
            ret = handler[0](self.node, cmd)

        return ret

    def apply_loop(self):
        i = 0
        while True:
            if self.node.shutdown_flag:
                break

            if i % 10 == 0:
                # print self.node.get_snapshot()
                pass
            i += 1

            if self.node.log.size() > CONF_LOG_MAX:
                self.node.checkpoint()

            item = self.node.log.pop(1)
            if item == None:
                continue

            cmd = item.cmd
            if isinstance(cmd, Future):
                cmd = cmd.cmd

            if self.node.index >= item.index:
                self.node.log_info('skip log [%d:%d]: "%s"' % (self.node.index, item.index, str(cmd)))
                continue

            self.node.log_debug('apply command [%d]: "%s"' % (item.index, str(cmd)))

            if cmd[0].lower() not in self.handler:
                self.node.log_error('unknown command: "%s"' % str(cmd))
                sys.exit(0)

            handler = self.handler[cmd[0].lower()]

            with self.node.data_lock:
                try:
                    ret = handler[0](self.node, cmd)
                except Exception as e:
                    ret = e

                self.node.index = item.index

            if isinstance(item.cmd, Future):
                item.cmd.set(ret)

    def process_work(self, sock):
        rio = resp.resp_io(sock)

        while True:
            if self.node.shutdown_flag:
                rio.close()
                return

            words = rio.read(1, split=True)
            if words == None:
                self.node.log_info('disconnected')
                rio.close()
                return

            if words == ['']:
                continue

            if len(words) > 0:
                if words[0].lower() not in self.handler:
                    rio.write(Exception('Unknown command: %s' % words[0]))
                    continue

                handler = self.handler[words[0].lower()]
                p_min = handler[2]
                p_max = handler[3]
                if len(words) - 1 < p_min:
                    rio.write(Exception('insufficient param'))
                    continue

                if p_max > 0 and len(words) - 1 > p_max:
                    rio.write(Exception('too many param'))
                    continue

                try:
                    ret = self.propose(words)
                except Exception as e:
                    ret = e

                if isinstance(ret, dict) and 'quit' in ret:
                    rio.close()
                    return

                rio.write(ret)


class RaftWorker(BaseWorker):
    def init_handler(self):
        super(RaftWorker, self).init_handler()

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
        '''
        self.handler['lpush'] = [self.do_lpush, 'we', 1, 1]
        self.handler['rpop'] = [self.do_rpop, 'we', 1, 1]
        self.handler['rpush'] = [self.do_rpush, 'we', 1, 1]
        self.handler['lpop'] = [self.do_lpop, 'we', 1, 1]
        self.handler['lrange'] = [self.do_lrange, 'r', 1, 1]
        self.handler['lindex'] = [self.do_lindex, 'r', 1, 1]
        self.handler['llen'] = [self.do_llen, 'r', 1, 1]
        self.handler['lset'] = [self.do_lset, 'we', 1, 1]
        self.handler['linsert'] = [self.do_linsert, 'we', 1, 1]
        self.handler['lrem'] = [self.do_lrem, 'we', 1, 1]
        self.handler['ltrim'] = [self.do_ltrim, 'we', 1, 1]
        '''

    # Set
    # TODO

    def do_hgetall(self, node, words):
        key = words[1].strip()
        self.node.check_ttl(key)

        result = []
        if key in self.node.data:
            hobj = self.node.data[key]

            if not isinstance(hobj, dict):
                raise ERROR_TYPE

            for k, v in hobj.items():
                result.append(k)
                result.append(v)

        return result

    def do_hdel(self, node, words):
        key = words[1].strip()
        self.node.check_ttl(key)

        count = 0
        if key in self.node.data:
            hobj = self.node.data[key]

            if not isinstance(hobj, dict):
                raise ERROR_TYPE

            if words[2] in hobj:
                del (hobj[words[2]])
                count += 1

        return count

    def do_hset(self, node, words):
        key = words[1].strip()
        self.node.check_ttl(key)

        if key not in self.node.data:
            self.node.data[key] = {}

        hobj = self.node.data[key]
        if not isinstance(hobj, dict):
            raise ERROR_TYPE

        hobj[words[2]] = words[3]

        return True

    def do_hlen(self, node, words):
        key = words[1].strip()
        self.node.check_ttl(key)

        if key in self.node.data:
            hobj = self.node.data[key]
            if not isinstance(hobj, dict):
                raise ERROR_TYPE

            return len(hobj)

        return 0

    def do_hget(self, node, words):
        key = words[1].strip()
        self.node.check_ttl(key)

        if key in self.node.data:
            hobj = self.node.data[key]

            if not isinstance(hobj, dict):
                raise ERROR_TYPE

            if words[2] not in hobj:
                raise ERROR_NOT_EXISTS

            return hobj[words[2]]

        return None

    def do_get(self, node, words):
        key = words[1].strip()
        self.node.check_ttl(key)

        if key in self.node.data:
            value = self.node.data[key]
            if not isinstance(value, str):
                raise ERROR_TYPE

            return value

        return None

    def do_del(self, node, words):
        key = words[1].strip()
        if key in self.node.data:
            del self.node.data[key]
            self.node.clear_ttl(key)
            return 1

        return 0

    def do_set(self, node, words):
        self.node.data[words[1]] = words[2]
        self.node.clear_ttl(words[1])
        return True

    # TODO: cleanup expired by backgroud thread
    def pexpireat(self, key, ts):
        ts = float(ts) / 1000.0
        return self.node.set_ttl(key, ts)

    def do_pexpireat(self, node, words):
        ts = intcast(words[2])
        if ts == None:
            raise ERROR_CAST

        return self.pexpreat(words[1], ts)

    def do_pexpire(self, node, words):
        msec = intcast(words[2])
        if msec == None:
            raise ERROR_CAST

        return self.pexpireat(words[1], int(time.time() * 1000) + msec)

    def do_expireat(self, node, words):
        ts = intcast(words[2])
        if ts == None:
            raise ERROR_CAST

        return self.pexpreat(words[1], ts * 1000)

    def do_expire(self, node, words):
        sec = intcast(words[2])
        if sec == None:
            raise ERROR_CAST

        return self.pexpireat(words[1], int(time.time() * 1000) + (sec * 1000))

