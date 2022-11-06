import struct

from pyraft.common import *
from pyraft.protocol.base import base_io
from pyraft.protocol.zk_exceptions import *

# came from kazoo serialization.py
# Struct objects with formats compiled
bool_struct = struct.Struct('B')
int_struct = struct.Struct('!i')
int_int_struct = struct.Struct('!ii')
int_int_long_struct = struct.Struct('!iiq')

int_long_int_long_struct = struct.Struct('!iqiq')
long_struct = struct.Struct('!q')
multiheader_struct = struct.Struct('!iBi')
reply_header_struct = struct.Struct('!iqi')
stat_struct = struct.Struct('!qqqqiiiqiiq')

session_map = {}
def get_session_io(session_id):
    return session_map.get(session_id)

def set_session_io(session_id, io):
    global session_map
    session_map[session_id] = io

def del_session_io(session_id):
    global session_map
    if session_id in session_map:
        del session_map[session_id]

def read_string(buffer, offset):
    """Reads an int specified buffer into a string and returns the
    string and the new offset in the buffer"""
    length = int_struct.unpack_from(buffer, offset)[0]
    offset += int_struct.size
    if length < 0:
        return None, offset
    else:
        index = offset
        offset += length
        return buffer[index:index + length].decode('utf-8'), offset

def read_acl(bytes, offset):
    perms = int_struct.unpack_from(bytes, offset)[0]
    offset += int_struct.size
    scheme, offset = read_string(bytes, offset)
    id, offset = read_string(bytes, offset)
    return (perms, scheme, id), offset

def write_string(bytes):
    if not bytes:
        return int_struct.pack(-1)
    else:
        utf8_str = bytes.encode('utf-8')
        return int_struct.pack(len(utf8_str)) + utf8_str

def write_buffer(bytes):
    if bytes is None:
        return int_struct.pack(-1)
    else:
        return int_struct.pack(len(bytes)) + bytes

def read_buffer(bytes, offset):
    length = int_struct.unpack_from(bytes, offset)[0]
    offset += int_struct.size
    if length < 0:
        return None, offset
    else:
        index = offset
        offset += length
        return bytes[index:index + length], offset

class ZkWatch:
    def __init__(self, type, state, path):
        self.type = type
        self.state = state
        self.path = path

    def serialize(self, b):
        b.extend(int_struct.pack(self.type))
        b.extend(int_struct.pack(self.state))
        b.extend(write_string(self.path))
        return b

class ZkClose:
    type = -11

    def deserialize(self, buff, offset):
        return ['close', self]

    def serialize(self, b):
        return b

class ZkConnect:
    type = None

    def __init__(self):
        self.connected = False

    def deserialize(self, buff, offset):
        self.proto_ver = int_struct.unpack_from(buff, offset)
        self.proto_ver, last_zxid, self.timeout, self.session_id = int_long_int_long_struct.unpack_from(buff, offset)
        offset += int_long_int_long_struct.size

        self.read_only = 0
        if self.proto_ver != -1: # relay connection skip password, read_only
            password, offset = read_buffer(buff, offset)
            self.read_only = buff[offset]

        self.connected = True
        return ['connect', self]

    def serialize(self, b):
        b.extend(int_int_long_struct.pack(self.proto_ver, self.timeout, self.session_id))
        b.extend(write_buffer(self.password))
        b.extend([self.read_only])
        return b

class ZkCreate:
    type = 1
    write = True # save original buffer & request socket to handle relay
    json = True # convert __dict__ to json for logging

    def deserialize(self, buff, offset):
        self.path, offset = read_string(buff, offset)
        data, offset = read_buffer(buff, offset)
        if data is None:
            self.data = ''
        else:
            self.data = data.decode('utf-8')

        acl_len = int_struct.unpack_from(buff, offset)[0]
        offset += int_struct.size

        self.acl = []
        for i in range(acl_len):
            perms = int_struct.unpack_from(buff, offset)[0]
            offset += int_struct.size
            id_scheme, offset = read_string(buff, offset)
            id_id, offset = read_string(buff, offset)
            self.acl.append([perms, id_scheme, id_id])

        self.flags = int_struct.unpack_from(buff, offset)[0]
        offset += int_struct.size

        return ['create', self]

    def serialize(self, b):
        b.extend(write_string(self.path))
        return b

class ZkDelete:
    type = 2
    write = True
    json = True

    def deserialize(self, buff, offset):
        self.path, offset = read_string(buff, offset)
        self.version = int_struct.unpack_from(buff, offset)[0]
        offset += int_struct.size
        return ['delete', self]

    def serialize(self, b):
        return b

class ZkExists:
    type = 3

    def deserialize(self, buff, offset):
        self.path, offset = read_string(buff, offset)
        self.watcher = (buff[offset] == 1)
        return ['exists', self]

    def serialize(self, b):
        b.extend(self._node.stat_pack())
        return b

class ZkGetData:
    type = 4

    def deserialize(self, buff, offset):
        self.path, offset = read_string(buff, offset)
        self.watcher = (buff[offset] == 1)
        return ['getdata', self]

    def serialize(self, b):
        b.extend(write_string(self._node.get_data()))
        b.extend(self._node.stat_pack())
        return b

class ZkSetData:
    type = 5
    write = True
    json = True

    def deserialize(self, buff, offset):
        self.path, offset = read_string(buff, offset)
        data, offset = read_buffer(buff, offset)
        if data is None:
            self.data = ''
        else:
            self.data = data.decode('utf-8')

        self.version = int_struct.unpack_from(buff, offset)[0]
        offset += int_struct.size
        return ['setdata', self]

    def serialize(self, b):
        b.extend(self._node.stat_pack())
        return b

class ZkGetACL:
    type = 6

    def deserialize(self, buff, offset):
        self.path, offset = read_string(buff, offset)
        return ['getacl', self]

    def serialize(self, b):
        count = len(self.acl)

        b.extend(int_struct.pack(count))
        if count <= 0:
            return b

        for acl in self.acl:
            b.extend(int_struct.pack(acl[0]))   # perms
            b.extend(write_string(acl[1]))      # scheme
            b.extend(write_string(acl[2]))      # id

        return b

class ZkSetACL:
    type = 7
    write = True
    json = True

    def deserialize(self, buff, offset):
        self.path, offset = read_string(buff, offset)
        acl_len = int_struct.unpack_from(buff, offset)[0]
        offset += int_struct.size

        self.acl = []
        for i in range(acl_len):
            perms = int_struct.unpack_from(buff, offset)
            offset += int_struct.size
            id_scheme, offset = read_string(buff, offset)
            id_id, offset = read_string(buff, offset)
            self.acl.append((perms, id_scheme, id_id))

        return ['setacl', self]

    def serialize(self, b):
        b.extend(self._node.stat_pack())
        return b

class ZkGetChildren:
    type = 8

    def deserialize(self, buff, offset):
        self.path, offset = read_string(buff, offset)
        self.watcher = (buff[offset] == 1)
        return ['getchildren', self]

    def serialize(self, b):
        count = len(self._children)

        b.extend(int_struct.pack(count))
        for child in self._children:
            b.extend(write_string(child.name))

        if self.type == 12: # getchildren2
            b.extend(self._node.stat_pack())

        return b

class ZkGetChildren2(ZkGetChildren):
    type = 12

class ZkPing:
    type = 11

    def deserialize(self, buff, offset):
        return ['ping', self]

    def serialize(self, b):
        return b

class zk_io(base_io):
    request_map = {
        -11:ZkClose,
        1:ZkCreate,
        2:ZkDelete,
        3:ZkExists,
        4:ZkGetData,
        5:ZkSetData,
        6:ZkGetACL,
        7:ZkSetACL,
        8:ZkGetChildren,
        11:ZkPing,
        12:ZkGetChildren2,
    }

    zxid = 0

    def __init__(self, sock):
        super().__init__(sock)
        self.conn = ZkConnect()
        self.xid = 0
        self.relay_map = {}

    def inc_zxid(self):
        self.zxid += 1
        return self.zxid

    def raw_encode(self, buff):
        return buff

    def encode(self, cmd):
        #print('>> encoding: %s' % str(cmd))
        if isinstance(cmd, bytes) or isinstance(cmd, bytearray):
            return cmd

        if isinstance(cmd, str): # like 4 letter command
            return cmd.encode()

        b = bytearray()
        # make header (if not connect)

        if isinstance(cmd, ZkConnect):
            logger.info("get new session: %s" % cmd.session_id)
            set_session_io(cmd.session_id, self)
        else:
            error_code = 0
            xid = self.xid
            if isinstance(cmd, ZookeeperError):
                error_code = cmd.code
            if isinstance(cmd, ZkPing):
                zxid = self.zxid
            if isinstance(cmd, ZkWatch):
                xid = -1
                zxid = -1
            else:
                zxid = self.inc_zxid()

            b.extend(reply_header_struct.pack(xid, zxid, error_code))

        # make body if not error
        if not isinstance(cmd, ZookeeperError):
            b = cmd.serialize(b)

        b = int_struct.pack(len(b)) + b
        return b

    def decode(self, buff):
        length = int_struct.unpack_from(buff, 0)[0]
        remain = buff[int_struct.size + length:]
        offset = int_struct.size

        if self.conn.connected == False:
            return self.conn.deserialize(buff, offset), remain

        self.xid, request_type = int_int_struct.unpack_from(buff, offset)
        offset += int_int_struct.size

        #print("decode buff: ",  self.sock.fileno(), bytes_to_str(buff))
        if request_type not in self.request_map:
            raise UnimplementedError()

        cmd = self.request_map[request_type]()
        cmd.xid = self.xid
        cmd.session_id = self.conn.session_id

        if hasattr(cmd, 'write'):
            cmd._original_buff = buff[:int_struct.size + length]
            cmd._req_io = self

        return cmd.deserialize(buff, offset), remain

    def decodable(self, buff):
        if len(buff) < 4:
            return False

        length = int_struct.unpack_from(buff, 0)[0]
        if len(buff) > (length + 4):
            return True

        return False

    # TODO: enhance
    def relay_cmd(self, addr, cmd): # cmd: ['create', ZkCreate]
        name = cmd[0]
        cmd = cmd[1]

        if addr not in self.relay_map:
            ip, port = addr.split(':')
            s = socket.socket()
            s.connect((ip, int(port)))
            self.relay_map[addr] = s

            # make relay connection
            b = bytearray()
            b.extend(int_struct.pack(int_long_int_long_struct.size))
            b.extend(int_long_int_long_struct.pack(-1, 0, 10000, cmd.session_id)) # proto, last zxid, timeout, session_id
            s.send(b)
            ret = s.recv(4096)

        s = self.relay_map[addr]
        s.send(cmd._original_buff)
        ret = s.recv(4096)

        return ret

    def close(self):
        super().close()
        del_session_io(self.conn.session_id)
