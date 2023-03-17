
import abc
from typing import Dict, List

import pandas as pd


class FileIngestion(abc.ABC):

  @abc.abstractmethod
  def io_batch_ingest_from_df(self, to_ingest: pd.DataFrame, media_columns: List['MediaColumn']):
    pass

  @abc.abstractmethod
  def io_batch_ingest(self, to_ingest: List[Dict]):
    pass

  @abc.abstractmethod
  def io_batch_digest(self, feature_table_name: str, to_digest: pd.DataFrame, media_columns: List['MediaColumn']):
    pass
class MediaColumn:

  def __init__(self, name: str, is_upload: bool):
    self.name = name
    self.is_upload = is_upload