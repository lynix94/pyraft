import time

from pyraft.common import *
from pyraft.worker.base_worker import BaseWorker


# worker for data handling with redis interface
class RedisWorker(BaseWorker):
    def __init__(self, addr):
        super(RedisWorker, self).__init__(addr)
        self.init_redis_handler()

    def init_redis_handler(self):
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

