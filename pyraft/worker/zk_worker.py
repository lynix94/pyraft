import time
import random
import struct

from pyraft.worker.worker import Worker
from pyraft.protocol import zk
from pyraft.protocol.zk_exceptions import *

stat_struct = struct.Struct('!qqqqiiiqiiq')

class ZkProtocol(object):
    def open_io(self, handle):
        return zk.zk_io(handle)

class ZkStat:
    def __init__(self):
        self.ctime = self.mtime = int(time.time() * 1000)
        self.czxid = self.mzxid = 0
        self.version = 0
        self.cversion = 0
        self.aversion = 0
        self.ephermeralOwner = 0
        self.pzxid = 0

# TODO: r/w locking
class ZkNode:
    def __init__(self, name, data, acl, flags):
        self.name = name
        self.data = data
        self.acl = acl
        self.flags = flags
        self.stat = ZkStat()
        self.children = {}

    def stat_pack(self):
        s = self.stat
        return stat_struct.pack(s.czxid, s.mzxid, s.ctime, s.mtime,
                                s.version, s.cversion, s.aversion,
                                s.ephermeralOwner, len(self.data), len(self.children),
                                s.pzxid)

    def get_data(self):
        return self.data

    def set_data(self, data):
        self.data = data
        self.stat.version += 1

    def get_acl(self):
        return self.acl

    def set_acl(self, acl):
        self.acl = acl
        self.stat.aversion += 1

    def get_flags(self):
        return self.flags

    def set_flags(self, flags):
        self.flags = flags

    def get_stat(self):
        self.stat

    def get_child(self, name):
        if name not in self.children:
            raise NoNodeError()

        return self.children[name]

    def create_child(self, name, data, acl, flags):
        if name in self.children:
            raise NodeExistsError()

        self.children[name] = ZkNode(name, data, acl, flags)
        self.stat.cversion += 1

    def delete_child(self, name):
        if name not in self.children:
            raise NoNodeError()

        print('DELETE>', self.name, name)
        child = self.children[name]
        if len(child.children) > 0:
            raise NotEmptyError()

        del self.children[name]
        self.stat.cversion += 1

    def get_children(self):
        print('getchildren>', self.name, self.children.keys())
        return self.children.values()

class ZkWorker(Worker):
    def __init__(self, addr):
        super(ZkWorker, self).__init__(addr)
        self.init_zk_handler()
        self.set_protocol(ZkProtocol())

    def init_node(self, node):
        if not hasattr(node, 'zk_root'):
            node.zk_root = ZkNode('root', 'root', [], 0)

    def init_zk_handler(self):
        self.handler['connect'] = [self.do_connect, 'r', 1, 1]
        self.handler['create'] = [self.do_create, 'we', 1, 1]
        self.handler['create2'] = [self.do_create, 'we', 1, 1]
        self.handler['exists'] = [self.do_exists, 'r', 1, 1]
        self.handler['delete'] = [self.do_delete, 'we', 1, 1]
        self.handler['getdata'] = [self.do_get_data, 'r', 1, 1]
        self.handler['setdata'] = [self.do_set_data, 'we', 1, 1]
        self.handler['getacl'] = [self.do_get_acl, 'r', 1, 1]
        self.handler['setacl'] = [self.do_set_acl, 'we', 1, 1]
        self.handler['getchildren'] = [self.do_get_children, 'r', 1, 1]
        self.handler['getchildren2'] = [self.do_get_children, 'r', 1, 1]
        self.handler['close'] = [self.do_close, 'r', 1, 1]
        '''
        self.handler['sync'] = [self.do_sync, 'we', 1, 1]
        self.handler['reconfig'] = [self.do_reconfig, 'we', 1, 1]
        self.handler['sasl'] = [self.do_sasl, 'r', 1, 1]
        self.handler['auth'] = [self.do_auth, 'r', 1, 1]
        self.handler['watch'] = [self.do_watch, 'r', 1, 1]
        self.handler['transaction'] = [self.do_transaction, 'r', 1, 1]
        self.handler['checkversion'] = [self.do_checkversion, 'r', 1, 1]
        '''

    def do_connect(self, node, words):
        cmd = words[1]
        cmd.session_id = random.getrandbits(63)
        cmd.password = bytearray([random.getrandbits(8) for i in range(16)])
        return cmd

    def do_close(self, node, words):
        cmd = words[1]
        return {'quit_after_send':cmd}

    def _cd_path(self, node, path, parent=False):
        if path[0] != '/':
            raise BadArgumentsError()

        if len(path) > 1 and path[-1] == '/':
            raise BadArgumentsError()

        dir_list = path[1:].split('/')
        cwd = node.zk_root
        count = len(dir_list)
        if parent:
            count -= 1

        for i in range(count):
            dir = dir_list[i]
            if dir == '':
                raise BadArgumentsError()

            dir = dir_list[i]
            cwd = cwd.get_child(dir)

        return cwd

    def do_create(self, node, words): # create, create2
        cmd = words[1]
        basename = cmd.path.split('/')[-1]
        cwd = self._cd_path(node, cmd.path, parent=True)
        cmd.child = cwd.create_child(basename, cmd.data, cmd.acl, cmd.flags)
        return cmd

    def do_delete(self, node, words):
        cmd = words[1]
        basename = cmd.path.split('/')[-1]
        cwd = self._cd_path(node, cmd.path, parent=True)
        cwd.delete_child(basename)
        return cmd

    def do_exists(self, node, words):
        cmd = words[1]
        cmd.node = self._cd_path(node, cmd.path)
        return cmd

    def do_get_data(self, node, words):
        cmd = words[1]
        cwd = self._cd_path(node, cmd.path)
        cmd.node = cwd
        return cmd

    def do_set_data(self, node, words):
        cmd = words[1]
        cwd = self._cd_path(node, cmd.path)
        cwd.set_data(cmd.data)
        return cmd

    def do_get_acl(self, node, words):
        cmd = words[1]
        cwd = self._cd_path(node, cmd.path)
        cmd.acl = cwd.get_acl()
        return cmd

    def do_set_acl(self, node, words):
        cmd = words[1]
        cwd = self._cd_path(node, cmd.path)
        cwd.set_acl(cmd.acl)
        return cmd

    def do_get_children(self, node, words): # get_children, get_children2
        cmd = words[1]
        cwd = self._cd_path(node, cmd.path)
        cmd.children = cwd.get_children()
        return cmd

    # TODO: transaction
    # TODO: checkversion
    # TODO: sync
    # TODO: reconfig
    # TODO: auth
    # TODO: sasl



