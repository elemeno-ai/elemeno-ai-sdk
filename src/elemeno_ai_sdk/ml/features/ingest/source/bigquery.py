from typing import Optional
import pandas as pd
import pandas_gbq

from elemeno_ai_sdk.config import Configs
from elemeno_ai_sdk.ml.features.ingest.source.base_source import BaseSource

MAX_LINES_TO_IMPORT = 10000


# TODO Add sql metadata validation
class BigQueryIngestionSource(BaseSource):

    def __init__(self,  gcp_project_id: Optional[str]=None, base_query: Optional[str]=None):
      """ Initialize the BigQuerySource
      This class is used internally by the feature table abstraction to ingest data from Big Query.
      
      args:
        
      - gcp_project_id: The GCP project id (optional). If specified will overwrite the yaml configuration.
      - base_query: The base query to be used to query the Big Query.
      """
      if base_query is not None:
        self.base_query = base_query
      cfg = Configs.instance()
      self.project_id = cfg.feature_store.source.params.project_id
      if gcp_project_id is not None:
        self.project_id = gcp_project_id

    def read(self, base_query: Optional[str] = None, **kwargs) -> pd.DataFrame:
      """ Reads the data from Big Query by running the query used when the object instance was created.

      returns:
      
      - A dataframe with the resulting data.
      """
      if base_query is not None:
        self.base_query = base_query
      return pandas_gbq.read_gbq(self.base_query, project_id=self.project_id)

    def read_after(self, timestamp_str: str, base_query: Optional[str] = None, **kwargs) -> pd.DataFrame:
      """ Reads the data from Big Query after a specificied timestamp.
      """
      if base_query is not None:
        self.base_query = base_query
      self.base_query = " {} WHERE event_timestamp > '{}' ORDER BY event_timestamp ASC LIMIT {}; " \
          .format(self.base_query, timestamp_str, MAX_LINES_TO_IMPORT)
      return self.read()