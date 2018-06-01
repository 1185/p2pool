import os
import platform

from twisted.internet import defer

from .. import data, helper
from p2pool.util import pack


P2P_PREFIX = '41babe56'.decode('hex')
P2P_PORT = 23333
ADDRESS_VERSION = 111
RPC_PORT = 23332
RPC_CHECK = defer.inlineCallbacks(lambda starwelsd: defer.returnValue(
            'terracoinaddress' in (yield starwelsd.rpc_help()) and
            (yield starwelsd.rpc_getblockchaininfo())['chain'] == 'test'
        ))
SUBSIDY_FUNC = lambda height: 20*100000000 >> (height + 1)//1050000
POW_FUNC = data.hash256
BLOCK_PERIOD = 120 # s
SYMBOL = 'tTRC'
CONF_FILE_FUNC = lambda: os.path.join(os.path.join(os.environ['APPDATA'], 'Terracoin') if platform.system() == 'Windows' else os.path.expanduser('~/Library/Application Support/Terracoin/') if platform.system() == 'Darwin' else os.path.expanduser('~/.terracoin'), 'terracoin.conf')
BLOCK_EXPLORER_URL_PREFIX = 'http://91.240.86.126:3001/testnet/block/'
ADDRESS_EXPLORER_URL_PREFIX = 'http://91.240.86.126:3001/testnet/address/'
TX_EXPLORER_URL_PREFIX = 'http://91.240.86.126:3001/testnet/tx/'
SANE_TARGET_RANGE = (2**256//2**32//1000 - 1, 2**256//2**32 - 1)
DUMB_SCRYPT_DIFF = 1
DUST_THRESHOLD = 1e8
