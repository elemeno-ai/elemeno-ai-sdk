
import abc
from typing import Dict, List

import pandas as pd


class FileIngestion(abc.ABC):

  @abc.abstractmethod
  def io_batch_ingest_from_df(self, to_ingest: pd.DataFrame, media_columns: List[str]):
    pass

  @abc.abstractmethod
  def io_batch_ingest(self, to_ingest: List[Dict]):
    pass