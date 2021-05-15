__version__ = '0.1.3.7.0'

import warnings

import uvloop

from flowsaber.core.context import *

uvloop.install()
warnings.filterwarnings('ignore', category=DeprecationWarning)
