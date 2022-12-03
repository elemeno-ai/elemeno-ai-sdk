__version__ = '0.3.28'

import logging
import os

__name__ = 'elemeno_ai_sdk'

LOGLEVEL = os.environ.get('LOGLEVEL', 'WARNING').upper()
logger = logging.getLogger()
logger.setLevel(level=LOGLEVEL)
print(f"Log level set to {LOGLEVEL}")
 