import os
import platform

from twisted.internet import defer

from .. import data, helper
from p2pool.util import pack


P2P_PREFIX = 'f9beb4d9'.decode('hex')
P2P_PORT = 8343
ADDRESS_VERSION = 0
RPC_PORT = 8342
RPC_CHECK = defer.inlineCallbacks(lambda starwelsd: defer.returnValue(
            (yield helper.check_genesis_block(starwelsd, '000000003d69a915e9da53348c5c272978bb743442e3a6341c11061c125811a2')) and
            (yield starwelsd.rpc_getblockchaininfo())['chain'] != 'test'
        ))
SUBSIDY_FUNC = lambda height: 50*100000000 >> (height + 1)//210000
POW_FUNC = data.hash256
BLOCK_PERIOD = 2 # s
SYMBOL = 'MAI'
CONF_FILE_FUNC = lambda: os.path.join(os.path.join(os.environ['APPDATA'], 'Starwels') if platform.system() == 'Windows' else os.path.expanduser('~/Library/Application Support/Starwels/') if platform.system() == 'Darwin' else os.path.expanduser('~/.starwels'), 'starwels.conf')
BLOCK_EXPLORER_URL_PREFIX = 'http://91.240.86.126:3001/block/'
ADDRESS_EXPLORER_URL_PREFIX = 'http://91.240.86.126:3001/address/'
TX_EXPLORER_URL_PREFIX = 'http://91.240.86.126:3001/tx/'
SANE_TARGET_RANGE = (2**256//2**32//1000000 - 1, 2**256//2**32 - 1)
DUMB_SCRYPT_DIFF = 1
DUST_THRESHOLD = 0.001e8
