
import abc
from typing import Dict, List

import pandas as pd


class FileIngestion(abc.ABC):

  @abc.abstractmethod
  def io_batch_ingest(self, to_ingest: List[Dict]):
    pass