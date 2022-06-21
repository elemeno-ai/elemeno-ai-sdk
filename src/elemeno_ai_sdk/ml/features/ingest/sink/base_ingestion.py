import abc
from typing import Dict, Optional, Any, List
import pandas as pd
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable

class Ingestion(abc.ABC):

  def __init__(self, **kwargs):
    self.ingestion_type = type(self).__name__

  @abc.abstractmethod
  def read_table(self, query: str) -> pd.DataFrame:
    pass

  @abc.abstractmethod
  def ingest(self, to_ingest: pd.DataFrame, ft: FeatureTable, renames: Optional[Dict[str, str]], 
      expected_columns: Optional[List[str]] = None, **kwargs) -> None:
    pass

  @abc.abstractmethod
  def create_table(self, to_ingest: pd.DataFrame, ft: FeatureTable, engine: Any, **kwargs) -> None:
    pass
  
  @abc.abstractmethod
  def ingest_from_query(self, query: str, ft: FeatureTable, **kwargs) -> None:
    pass
  
  @abc.abstractmethod
  def ingest_schema(self, feature_table: FeatureTable, schema_file_path: str, **kwargs) -> None:
    pass
  
  def with_ts_if_not_present(self, dataframe: pd.DataFrame, created_timestamp: str) -> pd.DataFrame:
    if (not created_timestamp in dataframe or
        len(dataframe[created_timestamp].isna()) == len(dataframe)):
      dataframe[created_timestamp] = pd.to_datetime('now', utc=True)
    return dataframe
    