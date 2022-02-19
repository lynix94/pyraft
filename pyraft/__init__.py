import logging

from pyraft import raft
from pyraft.protocol import resp

__all__ = ['raft', 'resp']
__version__ = '0.2.0'

logger = logging.getLogger('pyraft')

handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.setLevel(logging.WARN)

