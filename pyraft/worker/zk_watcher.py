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

    def __init__(self):
        self.child_watch_map = {}
        self.data_watch_map = {}

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
        if path in self.child_watch_map:
            session_list = self.child_watch_map[path].keys()
            for session in session_list:
                self.send_watch_notification(session, self.EVENT_CHILD, path)

            del self.child_watch_map[path]

        return []

    def check_data_watch(self, path):
        if path in self.data_watch_map:
            session_list = self.data_watch_map[path].keys()
            for session in session_list:
                self.send_watch_notification(session, self.EVENT_CHANGED, path)

            del self.data_watch_map[path]

    def regist_child_watch(self, path, session_id):
        if path not in self.child_watch_map:
            self.child_watch_map[path] = {}

        self.child_watch_map[path][session_id] = True

    def unregist_child_watch(self, path, session_id = None):
        if path not in self.child_watch_map:
            self.child_watch_map[path] = {}

        if session_id is None:
            del self.child_watch_map[path]
        else:
            del self.child_watch_map[path][session_id]

    def regist_data_watch(self, path, session_id):
        if path not in self.data_watch_map:
            self.data_watch_map[path] = {}

        self.data_watch_map[path][session_id] = True
        print(self.data_watch_map)

    def unregist_data_watch(self, path, session_id = None):
        if path not in self.data_watch_map:
            self.data_watch_map[path] = {}

        if session_id is None:
            del self.data_watch_map[path]
        else:
            del self.data_watch_map[path][session_id]

