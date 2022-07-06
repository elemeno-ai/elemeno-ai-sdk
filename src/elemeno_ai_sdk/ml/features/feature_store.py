

from datetime import datetime
from typing import Optional, Dict, List, Any, Union
import pandas as pd
import feast
from feast.infra.offline_stores.offline_store import RetrievalJob
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable
from elemeno_ai_sdk.ml.features.base_feature_store import BaseFeatureStore
from elemeno_ai_sdk.ml.features.ingest.source.elastic import ElasticIngestion
from elemeno_ai_sdk.ml.features.ingest.source.ingestion_source_builder import IngestionSourceType, IngestionSourceBuilder
from elemeno_ai_sdk.config import Configs
from elemeno_ai_sdk import logger
from elemeno_ai_sdk.ml.features.ingest.sink.ingestion_sink_builder \
   import IngestionSinkBuilder, IngestionSinkType

class FeatureStore(BaseFeatureStore):
  #TODO Bruno: add a new argument for the __init__ method to specify the feature source type
  def __init__(self, source_type: Optional[IngestionSourceType] = None, sink_type: Optional[IngestionSinkType] = None, **kwargs) -> None:
    """ 
    A FeatureStore is the starting point for working with Elemeno feature store via SDK.
    
    Use this class in conjunction with the FeatureTable class to create, read, update, and delete features.
    """
    self._elm_config = Configs.instance()
    self._fs = feast.FeatureStore(repo_path=self._elm_config.feature_store.feast_config_path)
    if not sink_type:
      logger.info("No sink type provided, read-only mode will be used")
    else:
      if sink_type == IngestionSinkType.BIGQUERY:
        self._sink = IngestionSinkBuilder().build_bigquery(self._fs)
      elif sink_type == IngestionSinkType.REDSHIFT:
        #TODO Bruno: Change this to create the connection string from the new redshift params from the config file
        self._sink = IngestionSinkBuilder().build_redshift(self._fs, self._get_connection_string())
      else:
        raise Exception("Unsupported sink type %s", sink_type)
    #TODO Bruno: add the logic to create the ElasticIngestion object when source_type from config is Elastic, or source type elastic was sent as an argument

    if not source_type:
      logger.info("No sink type provided.")
    else:
      if source_type == IngestionSourceType.ELASTIC:
        elastic_params = self._elm_config.feature_store.source.params
        self._source = IngestionSourceBuilder().build_elastic(**elastic_params)
      else:
        raise Exception("Unsupported source type %s", source_type)

    self.config = self._fs.config

  def _get_connection_string(self) -> str:
    host = self._elm_config.get('host')
    port = self._elm_config.get('port')
    database = self._elm_config.get('database')
    user = self._elm_config.get('user')
    password = self._elm_config.get('password')
    return f"jdbc:redshift://{host}:{port}/{database}?DBUser={user}&DBPassword={password}"

  @property
  def fs(self) -> feast.FeatureStore:
    return self._fs

  def ingest(self, feature_table: FeatureTable, 
      to_ingest: pd.DataFrame, renames: Optional[Dict[str, str]] = None,
      all_columns: Optional[List[str]] = None) -> None:
    """ 
    Ingest the given dataframe into the given feature table.
    
    This method allows you to rename the columns of the dataframe before ingesting.
    
    You can also filter the columns to be ingested.
    
    It is required that your dataframe have the timestamp columns (event_timestamp and created_timestamp) with the correct types (pd.DateTime).

    args:
    
    - feature_table: FeatureTable object
    - to_ingest: Dataframe to be ingested
    - renames: A dictionary of column names to be renamed.
    - all_columns: A list of columns to be ingested. If None, all columns will be ingested.
    """
    self._sink.ingest(to_ingest, feature_table, renames, all_columns)

  def ingest_from_query(self, ft: FeatureTable, query: str):
    """ 
    Ingest data from a query.
    
    It's important to notice that your query must return the timestamp columns (event_timestamp and created_timestamp) with the correct timestamp types of the source of choice.
    
    The query will be executed against the source of data you defined, so make sure query contains a compatible SQL statement.

    args:
    
    - ft: The FeatureTable object
    - query: A SQL query to ingest data from.
    """
    self._sink.ingest_from_query(query, ft)

  #TODO Bruno: remove host, username, password, and change this method to use the ElasticSource from self
  def ingest_from_elastic(self, feature_table: FeatureTable, index: str, query: str):
    """
    Ingest data from an Elasticsearch index.
    
    It's important to notice that your index must have the timestamp columns (event_timestamp and created_timestamp) with the correct timestamp types of the source of choice.
    
    You must specify an elasticsearch query. We only support vanila ES queries. Other types, like Lucene, may not work properly.
    
    Example:
    
    >>> query = {"query_string": {"query": "epoch_second(created_date)>0"}}

    args:
    
    - feature_table: FeatureTable object
    - index: The name of the Elasticsearch index
    - query: A query to ingest data from.
    """
    to_insert = self._source.read(index=index, query=query)
    all_columns = to_insert.columns.tolist()
    self._sink.ingest(to_insert, feature_table, all_columns)

  def get_historical_features(self, entity_source: pd.DataFrame, feature_refs: List[str]) -> RetrievalJob:
    """ Get historical features from the feature store.
    This method allows you to retrieve historical features from the feature store.
    
    You must specify a dataframe with the entity_id and entity_type columns.
    
    You must specify a list of feature_refs to retrieve. feature_refs are the name of the features.

    args:
    
    - entity_source: A dataframe with the entity_id and entity_type columns.
    - feature_refs: A list of feature_refs to retrieve.

    returns:
    
    - A RetrievalJob object.
    """
    return self._fs.get_historical_features(entity_source, feature_refs)

  def get_online_features(self, entities: List[Dict[str, Any]],
        requested_features: Optional[List[str]]=None) \
        -> feast.online_response.OnlineResponse:
    """ 
    Get online features from the feature store.
    This method allows you to retrieve online features from the feature store.
    
    You must specify a list of entities to retrieve.
    
    You may specify a list of features to retrieve. If None, all features will be retrieved.

    args:
    
    - entities: A list of entities to retrieve.
    - requested_features: Optional list of features to retrieve. If None, all features will be retrieved.
    
    returns:
    
    - An OnlineResponse object.
    """
    if self._fs.config.online_store is None:
      raise ValueError("Online store is not configure, make sure to configure the property online_store in the config yaml")
    return self._fs.get_online_features(features=requested_features, entity_rows=entities)
  
  def get_training_features(self, feature_table: FeatureTable,
        features_selected: List[str] = None,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        limit: Optional[int] = None) -> pd.DataFrame:
    """ 
    Get the training features for the given feature table.
    
    args:
    
    - feature_table: FeatureTable object
    - features_selected: A list of features to be selected. If None, all features will be selected.
    - date_from: The start date of the training period. If None, the start date of the feature table will be used.
    - date_to: The end date of the training period. If None, the end date of the feature table will be used.
    
    returns:
    
    A dataframe with the training features.
    """
    table_name = feature_table.name
    if features_selected is None:
      columns = "*"
    else:
      columns = ",".join(features_selected)
    where = ""
    if date_from:
      where += f"WHERE created_timestamp >= '{date_from.isoformat()}'"
    if date_to:´

  def ingest_schema(self, feature_table: FeatureTable, schema_file_path: str) -> None:
    """
    Ingest the schema of the feature table into the feature store. Useful when you're creating a new feature table.

    args:
    
    - feature_table: FeatureTable object
    - schema_file_path: The local path to the schema file.

    The is the first step to create a feature table via SDK, you will need to manually create a json schema file to your table.
    The schema file syntax uses an extension of JSONSchema. It's basically a JSONSchema file with some additional fields and properties.
    The additional fields are necessary in order to identify which of the columns are keys and how the timestamps will be identified.

    Example of a feature table schema file:

    >>> schema_example = {
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
    """
    self._sink.ingest_schema(feature_table, schema_file_path)

  def apply(self, objects: Union[feast.Entity, feast.FeatureView, feast.OnDemandFeatureView, feast.FeatureService,
    List[Union[feast.FeatureView, feast.OnDemandFeatureView, feast.Entity, feast.FeatureService]]],
        objects_to_delete: List[Union[feast.FeatureView, feast.OnDemandFeatureView, feast.Entity, feast.FeatureService, None]] = None,
        partial: bool = True):
    self._fs.apply(objects=objects, objects_to_delete=objects_to_delete, partial=partial)
