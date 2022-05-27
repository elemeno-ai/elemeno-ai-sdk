import abc
import enum
from typing import Union
import pandas as pd
from elemeno_ai_sdk.ml.features.feature_store import BaseFeatureStore
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable

class Ingestion(abc.ABC):

  def __init__(self, feature_store: BaseFeatureStore, **kwargs):
    self.ingestion_type = type(self).__name__

  @abc.abstractmethod
  def ingest(self, to_ingest: pd.DataFrame, ft: FeatureTable, **kwargs) -> None:
    pass
  
  @abc.abstractmethod
  def ingest_from_query(self, query: str, ft: FeatureTable, **kwargs) -> None:
    pass
  
  def with_ts_if_not_present(self, dataframe: pd.DataFrame, created_timestamp: str) -> pd.DataFrame:
    if (not created_timestamp in dataframe or
        len(dataframe[created_timestamp].isna()) == len(dataframe)):
      dataframe[created_timestamp] = pd.to_datetime('now', utc=True)
    return dataframe
    