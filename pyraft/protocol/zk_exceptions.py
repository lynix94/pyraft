from pyraft.common import RaftException
from collections import defaultdict

# referred and copied from kazoo exceptions

class ZkException(RaftException): # internal exception (not for response)
    def __init__(self, msg):
        super().__init__(msg)

class ZookeeperError(RaftException):
    def __init__(self):
        super().__init__('')

def _invalid_error_code():
    raise Exception('invalid error code')

EXCEPTIONS = defaultdict(_invalid_error_code)

def _zookeeper_exception(code):
    def decorator(klass):
        EXCEPTIONS[code] = klass
        klass.code = code
        return klass

    return decorator

@_zookeeper_exception(0)
class RolledBackError(ZookeeperError):
    pass

@_zookeeper_exception(-1)
class SystemZookeeperError(ZookeeperError):
    pass

@_zookeeper_exception(-2)
class RuntimeInconsistency(ZookeeperError):
    pass

@_zookeeper_exception(-3)
class DataInconsistency(ZookeeperError):
    pass

@_zookeeper_exception(-4)
class ConnectionLoss(ZookeeperError):
    pass

@_zookeeper_exception(-5)
class MarshallingError(ZookeeperError):
    pass

@_zookeeper_exception(-6)
class UnimplementedError(ZookeeperError):
    pass

@_zookeeper_exception(-7)
class OperationTimeoutError(ZookeeperError):
    pass

@_zookeeper_exception(-8)
class BadArgumentsError(ZookeeperError):
    pass

@_zookeeper_exception(-13)
class NewConfigNoQuorumError(ZookeeperError):
    pass

@_zookeeper_exception(-14)
class ReconfigInProcessError(ZookeeperError):
    pass

@_zookeeper_exception(-100)
class APIError(ZookeeperError):
    pass

@_zookeeper_exception(-101)
class NoNodeError(ZookeeperError):
    pass

@_zookeeper_exception(-102)
class NoAuthError(ZookeeperError):
    pass

@_zookeeper_exception(-103)
class BadVersionError(ZookeeperError):
    pass

@_zookeeper_exception(-108)
class NoChildrenForEphemeralsError(ZookeeperError):
    pass

@_zookeeper_exception(-110)
class NodeExistsError(ZookeeperError):
    pass

@_zookeeper_exception(-111)
class NotEmptyError(ZookeeperError):
    pass

@_zookeeper_exception(-112)
class SessionExpiredError(ZookeeperError):
    pass

@_zookeeper_exception(-113)
class InvalidCallbackError(ZookeeperError):
    pass

@_zookeeper_exception(-114)
class InvalidACLError(ZookeeperError):
    pass

@_zookeeper_exception(-115)
class AuthFailedError(ZookeeperError):
    pass

@_zookeeper_exception(-118)
class SessionMovedError(ZookeeperError):
    pass

@_zookeeper_exception(-119)
class NotReadOnlyCallError(ZookeeperError):
    """An API call that is not read-only was used while connected to
    a read-only server"""

class ConnectionClosedError(SessionExpiredError):
    """Connection is closed"""

