import json
import random
import struct

from pyraft.common import *
from pyraft.worker.worker import Worker
from pyraft.protocol import zk
from pyraft.protocol.zk_exceptions import *
from pyraft.worker.zk_watcher import ZkWatcher
from pyraft.worker.zk_ephemeral import ZkEphermeralManager

stat_struct = struct.Struct('!qqqqiiiqiiq')

class ZkProtocol:
    def open_io(self, handle):
        return zk.zk_io(handle)

# TODO: r/w locking
def init_zk_stat():
    stat = {}
    stat['ctime'] = stat['mtime'] = int(time.time() * 1000)
    stat['czxid'] = stat['mzxid'] = 0
    stat['version'] = 0
    stat['cversion'] = 0
    stat['aversion'] = 0
    stat['ephermeralOwner'] = 0
    stat['pzxid'] = 0

    return stat

class ZkNode(object):
    def __init__(self, name, obj):
        self.name = name
        self.obj = obj

    def __getattr__(self, name):
        if name in self.obj:
            return self.obj[name]

        raise Exception('%s not found' % name)

    def __setattr__(self, key, value):
        if key in ['name', 'obj']:
            super().__setattr__(key, value)
        else:
            self.obj[key] = value

    def is_ephemeral(self):
        return (self.flags & 1) != 0

    def is_sequence(self):
        return (self.flags & 2) != 0

    def stat_pack(self):
        s = self.stat
        n_child = 0
        for v in self.obj.values():
            if isinstance(v, dict):
                n_child += 1

        return stat_struct.pack(s['czxid'], s['mzxid'], s['ctime'], s['mtime'],
                                s['version'], s['cversion'], s['aversion'],
                                s['ephermeralOwner'], len(self.data), n_child,
                                s['pzxid'])

    def get_data(self):
        return self.data

    def set_data(self, data):
        self.data = data
        self.stat['version'] += 1

    def get_acl(self):
        return self.acl

    def set_acl(self, acl):
        self.acl = acl
        self.stat['aversion'] += 1

    def get_flags(self):
        return self.flags

    def set_flags(self, flags):
        self.flags = flags

    def get_child(self, name):
        if name not in self.obj:
            raise NoNodeError()

        if not isinstance(self.obj[name], dict):
            raise NoNodeError()

        return ZkNode(name, self.obj[name])

    def create_child(self, name, data, acl, flags):
        if name in ['data', 'acl', 'flags', 'stat']:
            logger.error('%s is not allowed in zk tree' % name)
            raise BadArgumentsError()

        if name in self.obj:
            raise NodeExistsError()

        self.obj[name] = {'data':data, 'acl':acl, 'flags':flags, 'stat':init_zk_stat()}
        self.stat['cversion'] += 1
        return ZkNode(name, self.obj[name])

    def delete_child(self, name):
        if name not in self.obj:
            raise NoNodeError()

        child = self.obj[name]

        for k, v in child.items():
            if k != 'stat' and isinstance(v, dict):
                raise NotEmptyError()

        del self.obj[name]
        self.stat['cversion'] += 1

    def get_children(self):
        ret = []
        for k, v in self.obj.items():
            if k != 'stat' and isinstance(v, dict):
                ret.append(ZkNode(k, v))

        return ret

# handle string based request (by append_entry)
def handle_json(handler):
    convert_table = {
        'create':zk.ZkCreate,
        'delete':zk.ZkDelete,
        'setdata':zk.ZkSetData,
        'setacl':zk.ZkSetACL
    }

    def convert_to_command(*args, **kwargs): # self, node, words (str, cmd)
        words = args[2]
        if not isinstance(words[1], str):
            return handler(*args, **kwargs)

        cmd = convert_table[words[0]]()
        obj = json.loads(words[1])
        cmd.__dict__.update(obj)

        words[1] = cmd
        return handler(*args, **kwargs)

    return convert_to_command

class ZkWorker(Worker):
    def __init__(self, addr):
        super().__init__(addr)
        self.init_zk_handler()
        self.set_protocol(ZkProtocol())
        self.session_map = {} # connected sessions

    def start(self, node):
        super().start(node)
        self.ephemeral_mgr = ZkEphermeralManager(node)
        self.ephemeral_mgr.start()
        self.watch_mgr = ZkWatcher(node)

    def shutdown(self):
        super().shutdown()
        self.ephemeral_mgr.quit_flag = True

    def join(self):
        super().join()
        self.ephemeral_mgr.join()

    def init_node(self, node):
        if 'ZK' not in node.data:
            node.data['ZK'] = {'data':'', 'acl':[], 'flags':0, 'stat':init_zk_stat()}

        self.node = node

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
        self.handler['ping'] = [self.do_ping, 'r', 1, 1]
        '''
        self.handler['sync'] = [self.do_sync, 'we', 1, 1]
        self.handler['reconfig'] = [self.do_reconfig, 'we', 1, 1]
        self.handler['sasl'] = [self.do_sasl, 'r', 1, 1]
        self.handler['auth'] = [self.do_auth, 'r', 1, 1]
        self.handler['transaction'] = [self.do_transaction, 'r', 1, 1]
        self.handler['checkversion'] = [self.do_checkversion, 'r', 1, 1]
        '''

    def do_connect(self, node, words):
        cmd = words[1]
        if cmd.session_id == 0:
            cmd.session_id = random.getrandbits(63)

        cmd.password = bytearray([random.getrandbits(8) for i in range(16)])

        node.request('hset', 'zk_session', str(cmd.session_id), str(int(time.time())))
        return cmd

    def do_close(self, node, words):
        cmd = words[1]

        node.request('hdel', 'zk_session', str(cmd.session_id))
        return {'quit_after_send':cmd}

    def _cd_path(self, node, path, parent=False):
        if path[0] != '/':
            raise BadArgumentsError()

        if len(path) > 1 and path[-1] == '/':
            raise BadArgumentsError()

        if path.strip() == '/':
            return ZkNode('ZK', node.data['ZK'])

        dir_list = path[1:].split('/')
        cwd = ZkNode('ZK', node.data['ZK'])
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

    @handle_json
    def do_create(self, node, words): # create, create2
        cmd = words[1]
        basename = cmd.path.split('/')[-1]
        cwd = self._cd_path(node, cmd.path, parent=True)
        cmd._child = cwd.create_child(basename, cmd.data, cmd.acl, cmd.flags)

        self.watch_mgr.check_data_watch(cmd.path, ZkWatcher.EVENT_CREATED)
        self.watch_mgr.check_child_watch(cmd.path)

        if cmd._child.is_ephemeral():
            cmd._child.stat['ephemeralOwner'] = cmd.session_id
            self.ephemeral_mgr.regist(cmd.path, cmd._child)

        return cmd

    @handle_json
    def do_delete(self, node, words):
        cmd = words[1]
        basename = cmd.path.split('/')[-1]

        cwd = self._cd_path(node, cmd.path)
        if cwd.is_ephemeral():
            self.ephemeral_mgr.unregist(cmd.path)

        cwd = self._cd_path(node, cmd.path, parent=True)
        cwd.delete_child(basename)

        parent_path = '/'.join(cmd.path.split('/')[:-1]) # '/aaa/bbb' -> '/aaa'
        if parent_path == '':
            parent_path = '/'

        self.watch_mgr.check_data_watch(cmd.path, ZkWatcher.EVENT_DELETED)
        self.watch_mgr.check_child_watch(parent_path)

        return cmd

    def do_exists(self, node, words):
        cmd = words[1]
        if cmd.watcher:
            self.watch_mgr.regist_data_watch(cmd.path, cmd.session_id)

        cmd._node = self._cd_path(node, cmd.path)
        return cmd

    def do_get_data(self, node, words):
        cmd = words[1]
        cwd = self._cd_path(node, cmd.path)
        cmd._node = cwd
        if cmd.watcher:
            self.watch_mgr.regist_data_watch(cmd.path, cmd.session_id)

        return cmd

    @handle_json
    def do_set_data(self, node, words):
        cmd = words[1]
        cwd = self._cd_path(node, cmd.path)
        cwd.set_data(cmd.data)
        cmd._node = cwd

        self.watch_mgr.check_data_watch(cmd.path, ZkWatcher.EVENT_CHANGED)
        return cmd

    def do_get_acl(self, node, words):
        cmd = words[1]
        cwd = self._cd_path(node, cmd.path)
        cmd.acl = cwd.get_acl()
        return cmd

    @handle_json
    def do_set_acl(self, node, words):
        cmd = words[1]
        cwd = self._cd_path(node, cmd.path)
        cwd.set_acl(cmd.acl)
        cmd._node = cwd
        return cmd

    def do_get_children(self, node, words): # get_children, get_children2
        cmd = words[1]
        cwd = self._cd_path(node, cmd.path)
        cmd._children = cwd.get_children()
        cmd._node = cwd

        if cmd.watcher:
            self.watch_mgr.regist_child_watch(cmd.path, cmd.session_id)

        return cmd

    def do_ping(self, node, words):
        cmd = words[1]
        logger.debug('process ping session: %s' % cmd.session_id)
        node.request('hset', 'zk_session', str(cmd.session_id), str(int(time.time())))
        return cmd

    def relay_cmd(self, leader, cmd, worker_offset): # cmd: ['create', ZkCreate]
        p = leader
        try:
            ip, port = p.addr.split(':')
            new_addr = '%s:%d' % (ip, int(port)+worker_offset)
            ret = cmd[1]._req_io.relay_cmd(new_addr, cmd)
            return {'bypass':ret}

        except Exception as e:
            raise RaftException('relay to leader has exception: %s' % str(e))

    # TODO: transaction
    # TODO: checkversion
    # TODO: sync
    # TODO: reconfig
    # TODO: auth
    # TODO: sasl
    # TODO: sequence
