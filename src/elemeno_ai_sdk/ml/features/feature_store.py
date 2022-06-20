

from datetime import datetime
from typing import Optional, Dict, List, Any, Union
import pandas as pd
import feast
from feast.infra.offline_stores.offline_store import RetrievalJob
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable
from elemeno_ai_sdk.ml.features.base_feature_store import BaseFeatureStore
from elemeno_ai_sdk.ml.features.ingest.source.elastic import ElasticIngestion
from elemeno_ai_sdk.config import Configs
from elemeno_ai_sdk import logger
from elemeno_ai_sdk.ml.features.ingest.sink.ingestion_sink_builder \
   import IngestionSinkBuilder, IngestionSinkType

class FeatureStore(BaseFeatureStore):
  """ A FeatureStore is the starting point for working with Elemeno feature store via SDK.
    Use this class in conjunction with the FeatureTable class to create, read, update, and delete features.
  """
  def __init__(self, sink_type: Optional[IngestionSinkType] = None, **kwargs) -> None:
    self._elm_config = Configs.instance()
    self._fs = feast.FeatureStore(repo_path=self._elm_config.feature_store.feast_config_path)
    if not sink_type:
      logger.info("No sink type provided, read-only mode will be used")
    else:
      if sink_type == IngestionSinkType.BIGQUERY:
        self._sink = IngestionSinkBuilder().build_bigquery(self._fs)
      elif sink_type == IngestionSinkType.REDSHIFT:
        self._sink = IngestionSinkBuilder().build_redshift(self._fs, kwargs['connection_string'])
      else:
        raise Exception("Unsupported sink type %s", sink_type)
    self.config = self._fs.config

  @property
  def fs(self) -> feast.FeatureStore:
    return self._fs

  def ingest(self, feature_table: FeatureTable, 
      to_ingest: pd.DataFrame, renames: Optional[Dict[str, str]] = None,
      all_columns: Optional[List[str]] = None) -> None:
    self._sink.ingest(to_ingest, feature_table, renames, all_columns)

  def ingest_from_query(self, ft: FeatureTable, query: str):
    self._sink.ingest_from_query(query, ft)

  def ingest_from_elastic(self, feature_table: FeatureTable, index: str,
      query: str, host: str, username: str, password: str):
    elastic_source = ElasticIngestion(host=host, username=username, password=password)
    to_insert = elastic_source.read(index=index, query=query)
    all_columns = to_insert.columns.tolist()
    self._sink.ingest(to_insert, feature_table, all_columns)

  def get_historical_features(self, entity_source: pd.DataFrame, feature_refs: List[str]) -> RetrievalJob:
    return self._fs.get_historical_features(entity_source, feature_refs)

  def get_online_features(self, entities: List[Dict[str, Any]],
        requested_features: Optional[List[str]]=None) \
        -> feast.online_response.OnlineResponse:
    if self._fs.config.online_store is None:
      raise ValueError("Online store is not configure, make sure to configure the property online_store in the config yaml")
    return self._fs.get_online_features(features=requested_features, entity_rows=entities)
  
  def get_training_features(self, feature_table: FeatureTable,
        features_selected: List[str] = None,
        from_: Optional[datetime] = None,
        to_: Optional[datetime] = None,
        limit: Optional[int] = None) -> pd.DataFrame:
    """ Get the training features for the given feature table.
    Args:
      feature_table: FeatureTable object
      features_selected: A list of features to be selected. If None, all features will be selected.
      from_: The start date of the training period. If None, the start date of the feature table will be used.
      to_: The end date of the training period. If None, the end date of the feature table will be used.
    Returns:
      A dataframe with the training features.
    """
    table_name = feature_table.name
    if features_selected is None:
      columns = "*"
    else:
      columns = ",".join(features_selected)
    where = ""
    if from_:
      where += f"WHERE created_timestamp >= '{from_.isoformat()}'"
    if to_:
      if where != "":
        where += f" AND created_timestamp <= '{to_.isoformat()}'"
      else:
        where += f"WHERE created_timestamp <= '{to_.isoformat()}'"
    if limit:
      where += f" LIMIT {limit}"
    query = f"SELECT {columns} FROM {table_name} {where}"
    print(query)
    return self._sink.read_table(query)

  def ingest_schema(self, feature_table: FeatureTable, schema_file_path: str) -> None:
    """
    Ingest the schema of the feature table into the feature store. Useful when you're creating a new feature table.

    Args:
      feature_table: FeatureTable object
      schema_file_path: The local path to the schema file.

    The is the first step to create a feature table via SDK, you will need to manually create a json schema file to your table.
    The schema file syntax uses an extension of JSONSchema. It's basically a JSONSchema file with some additional fields and properties.
    The additional fields are necessary in order to identify which of the columns are keys and how the timestamps will be identified.

    Example of a feature table schema file:

    ```
    {
      "type": "object",
      "properties": {
        "userId": {
          "isKey": "true",
          "type": "string"
        },
        "address": {
          "type": "object"
        },
        "medianSpent": {
          "type": "number"
        }
        "age": {
          "type": "integer"
        },
        "created_timestamp": {
          "examples": [
            "1970-01-01T00:00:00+00:00"
          ],
          "format": "date-time",
          "type": "string"
        },
        "event_timestamp": {
          "examples": [
            "1970-01-01T00:00:00+00:00"
          ],
          "format": "date-time",
          "type": "string"
        }
      }
    }
    ```
    """
    self._sink.ingest_schema(feature_table, schema_file_path)

  def apply(self, objects: Union[feast.Entity, feast.FeatureView, feast.OnDemandFeatureView, feast.FeatureService,
    List[Union[feast.FeatureView, feast.OnDemandFeatureView, feast.Entity, feast.FeatureService]]],
        objects_to_delete: List[Union[feast.FeatureView, feast.OnDemandFeatureView, feast.Entity, feast.FeatureService, None]] = None,
        partial: bool = True):
    self._fs.apply(objects=objects, objects_to_delete=objects_to_delete, partial=partial)
