__version__ = '0.0.42'

import logging

LOGLEVEL = os.environ.get('LOGLEVEL', 'WARNING').upper()
logger = logging.getLogger()
logger.setLevel(level=LOGLEVEL)
print(f"Log level set to {LOGLEVEL}")
from . import datasources
