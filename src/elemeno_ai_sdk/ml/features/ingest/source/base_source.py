import abc
import pandas as pd

class BaseSource(abc.ABC):

  def __init__(self, **kwargs):
    self.source_type = type(self).__name__
  
  @abc.abstractmethod
  def read(self, **kwargs) -> pd.DataFrame:
    pass

  @abc.abstractmethod
  def read_after(self, timestamp_str: str, **kwargs) -> pd.DataFrame:
    pass