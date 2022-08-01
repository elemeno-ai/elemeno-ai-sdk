import abc
from typing import Dict, List, Optional
import pandas as pd


class ReadResponse:
  def __init__(self, dataframe: pd.DataFrame, prepared_medias: Optional[str]):
    self.dataframe = dataframe
    self.prepared_medias = prepared_medias
class BaseSource(abc.ABC):

  def __init__(self, **kwargs):
    self.source_type = type(self).__name__
  
  @abc.abstractmethod
  def read(self, **kwargs) -> ReadResponse:
    pass

  @abc.abstractmethod
  def read_after(self, timestamp_str: str, **kwargs) -> ReadResponse:
    pass