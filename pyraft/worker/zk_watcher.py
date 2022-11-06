from pyraft.common import *
from pyraft.protocol import zk

class ZkWatcher:
    EVENT_NONE = -1
    EVENT_CREATED = 1
    EVENT_DELETED = 2
    EVENT_CHANGED = 3
    EVENT_CHILD = 4

    STATE_UNKNOWN = -1
    STATE_DISCONNECTED = 0
    STATE_NO_SYNC_CONNECTED = 1 # deprecated
    STATE_SYNC_CONNECTED = 3
    STATE_AUTH_FAILED = 4
    STATE_CONNECTED_READ_ONLY = 5
    STATE_SASL_AUTHENTICATED = 6
    STATE_CLOSED = 7
    STATE_EXPIRED = -112

    def __init__(self, node):
        self.lock = threading.Lock()
        self.node = node

    def send_watch_notification(self, session, event, path):
        print('>> send watch notification to', session, event, path)
        io = zk.get_session_io(session)
        if io is None:
            print('no session for watch')
            #TODO: warning
            return

        noti = zk.ZkWatch(event, self.STATE_SYNC_CONNECTED, path)
        io.write(noti)

    def check_child_watch(self, path):
        with self.lock:
            session_list = self.node.data.get('zk_watch_child_%s' % path)
            if session_list == None:
                return

            for session in session_list:
                self.send_watch_notification(session, self.EVENT_CHILD, path)

            del self.node.data['zk_watch_child_%s' % path]

        return []

    def check_data_watch(self, path, event):
        with self.lock:
            session_list = self.node.data.get('zk_watch_data_%s' % path)
            if session_list == None:
                return

            for session in session_list:
                self.send_watch_notification(session, event, path)

            del self.node.data['zk_watch_data_%s' % path]

        return []

    def regist_child_watch(self, path, session_id):
        with self.lock:
            self.node.request_async('rpush', 'zk_watch_child_%s' % path, session_id)

    def regist_data_watch(self, path, session_id):
        with self.lock:
            self.node.request_async('rpush', 'zk_watch_data_%s' % path, session_id)
