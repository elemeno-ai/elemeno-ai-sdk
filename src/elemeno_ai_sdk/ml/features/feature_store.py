

from datetime import datetime
import json
from typing import Optional, Dict, List, Any, Union
import pandas as pd
import feast
from feast.infra.offline_stores.offline_store import RetrievalJob
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable
from elemeno_ai_sdk.config import Configs
from elemeno_ai_sdk import logger
from elemeno_ai_sdk.ml.features.ingest.sink.file_ingestion import FileIngestion
from elemeno_ai_sdk.ml.features.ingest.sink.ingestion_sink_builder \
   import FileIngestionSinkType, IngestionSinkBuilder, IngestionSinkType
from elemeno_ai_sdk.ml.features.ingest.sink.minio_ingestion import MinioIngestion
from elemeno_ai_sdk.ml.features.ingest.source.ingestion_source_builder \
  import IngestionSourceBuilder, IngestionSourceType
from elemeno_ai_sdk.ml.features.ingest.source.base_source import ReadResponse

class FeatureStore:

  def __init__(self, 
    sink_type: Optional[IngestionSinkType] = None,
    file_sink_type: Optional[FileIngestionSinkType] = None,
    source_type: Optional[IngestionSourceType] = None, **kwargs) -> None:
    """ 
    A FeatureStore is the starting point for working with Elemeno feature store via SDK.
    
    Use this class in conjunction with the FeatureTable class to create, read, update, and delete features.
    """
    self._elm_config = Configs.instance()
    self._fs = feast.FeatureStore(repo_path=self._elm_config.feature_store.feast_config_path, )
    if not sink_type:
      logger.info("No sink type provided, read-only mode will be used")
    else:
      self._sink_type = sink_type
      if sink_type == IngestionSinkType.BIGQUERY:
        self._sink = IngestionSinkBuilder().build_bigquery(self._fs)
      elif sink_type == IngestionSinkType.REDSHIFT:
        redshift_params = self._elm_config.feature_store.sink.params
        self._sink = IngestionSinkBuilder().build_redshift(self._fs, self._get_connection_string(redshift_params))
      elif sink_type == IngestionSinkType._REDSHIFT_UNIT_TESTS:
        redshift_params = self._elm_config.feature_store.sink.params
        self._sink = IngestionSinkBuilder()._build_redshift_unit_tests(self._fs, self._get_connection_string(redshift_params))
      else:
        raise Exception("Unsupported sink type %s", sink_type)
    if not source_type:
      logger.info("No source type provided, read-only mode will be used")
    else:
      if source_type == IngestionSourceType.BIGQUERY:
        self._source = IngestionSourceBuilder().build_big_query()
      elif source_type == IngestionSourceType.ELASTIC:
        self._source = IngestionSourceBuilder().build_elastic()
      elif source_type == IngestionSourceType.REDSHIFT:
        self._source = IngestionSourceBuilder().build_redshift()
      elif source_type == IngestionSourceType.GCS:
        self._source = IngestionSourceBuilder().build_gcs()
      else:
        raise Exception("Unsupported source type %s", source_type)
    self._file_sink_type = file_sink_type
    if not file_sink_type:
      logger.info("File sink type not specified, will not download files when there are binary columns")
    else:
      if not self._elm_config.dask.uri:
        logger.warn("Dask URI not specified, will not download files")
      else:
        print("Creating instance of MinioIngestionDask")
        self. _file_sink = MinioIngestion(self._elm_config.dask.uri)
    self.config = self._fs.config
    # memory holds a reference for the result of the last data handling method
    self._memory = None

  def read_from_query(self, query: str) -> pd.DataFrame:
    return super().read_from_query(self._source, query)

  def _get_connection_string(self, redshift_params):
    if self._sink_type == IngestionSinkType._REDSHIFT_UNIT_TESTS:
      return "sqlite:///test.db"
    user = redshift_params.user
    password = redshift_params.password
    host = redshift_params.host
    port = redshift_params.port 
    database = redshift_params.database 
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"

  @property
  def fs(self) -> feast.FeatureStore:
    return self._fs

  def ingest_response(self, feature_table: FeatureTable, 
      to_ingest: ReadResponse, renames: Optional[Dict[str, str]] = None,
      all_columns: Optional[List[str]] = None) -> None:
    """ 
    Ingest the given dataframe into the given feature table.
    
    This method allows you to rename the columns of the dataframe before ingesting.
    
    You can also filter the columns to be ingested.
    
    It is required that your dataframe have the timestamp columns (event_timestamp and created_timestamp) with the correct types (pd.DateTime).

    args:
    
    - feature_table: FeatureTable object
    - to_ingest: A ReadResponse instance (since base_source.py ReadResponse)
    - renames: A dictionary of column names to be renamed.
    - all_columns: A list of columns to be ingested. If None, all columns will be ingested.
    """
    self._ingest_files(to_ingest)
    self.ingest(feature_table, to_ingest.dataframe, renames, all_columns)

  def staging_ingest(self, to_ingest: pd.DataFrame, name: str) -> None:
    """ 
    Ingest the given dataframe into a staging are in the feature store batch persistence

    The ingest data will not be a feature table yet, instead it will be available as 
    a simple structure in the persistence, and can be later turned into a feature table
    using the staging_to_feature_table method.

    args:
    
    - to_ingest: A dataframe to be ingested
    - name: The name of the staging area
    """
    self._sink.staging_ingest(to_ingest, name)

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
    - to_ingest: A pandas dataframe to be ingested
    - renames: A dictionary of column names to be renamed
    - all_columns: A list of columns to be ingested. If None, all columns will be ingested
    """
    self._sink.ingest(to_ingest, feature_table, renames, all_columns)
    
  def _ingest_files(self, to_ingest: ReadResponse):
    print("file sink type")
    print(self._file_sink_type)
    if self._file_sink_type is not None:
      medias = json.loads(to_ingest.prepared_medias)
      print("len of to_ingest.prepared_medias", len(medias))
      self._file_sink.io_batch_ingest(medias)
  
  def ingest_from_query_same_source(self, ft: FeatureTable, query: str):
    """ 
    Ingest data from a query. Used when the source and the sink are the same.
    
    It's important to notice that your query must return the timestamp columns (event_timestamp and created_timestamp) with the correct timestamp types of the source of choice.
    
    The query will be executed against the source of data you defined, so make sure query contains a compatible SQL statement.

    args:
    
    - ft: The FeatureTable object
    - query: A SQL query to ingest data from.
    """
    self._sink.ingest_from_query(query, ft)

  def read_and_ingest_from_query(self, ft: 'FeatureTable', query: str, 
      binary_cols: List[str], ignore_when_empty: Optional[List[str]] = None, **kwargs):
    """
    Ingest data from a query. Used when the source and the sink are different.
    
    It's important to notice that your query must return the timestamp columns (event_timestamp and created_timestamp) with the correct timestamp types of the source of choice.
    
    The query will be executed against the source of data you defined, so make sure query contains a compatible SQL statement.

    args:
    
    - ft: The FeatureTable object
    - query: A SQL query to ingest data from.
    - binary_cols: A list of columns that are binary and will be downloaded to cloud object storage.
    - ignore_when_empty: A list of columns that will be ignored when the result is empty.
    """
    if not 'index' in kwargs:
      raise("index must be provided")
    
    read_response = self._source.read(query=query, binary_columns=binary_cols, dest_folder_col="id", media_id_col="id", **kwargs)
    # make sure only the featuretable columns are ingested
    cols = [e.name for e in ft.entities]
    cols.extend([f.name for f in ft.features])
    cols.extend([ft.created_col, ft.evt_col])
    if ignore_when_empty != None:
      read_response.dataframe = read_response.dataframe.dropna(subset=ignore_when_empty)
    self.ingest_response(ft, read_response, all_columns=cols)

  def read_and_ingest_from_query_after(self, ft: 'FeatureTable', query: str, after: str, binary_cols: Optional[List[str]] = None,
      ignore_when_empty: Optional[List[str]] = None, **kwargs):
    """
    Ingest data from a query after a specific timestamp. Used when the source and the sink are different.

    It's important to notice that your query must return the timestamp columns (event_timestamp and created_timestamp) with the correct timestamp types of the source of choice.
    
    The query will be executed against the source of data you defined, so make sure query contains a compatible SQL statement.

    args:
    - ft: The FeatureTable object
    - query: A SQL query to ingest data from.
    - after: A timestamp after which the query will be executed. Use the same date format of the source.
    - binary_cols: A list of binary columns containing references to files to be downloaded to our cloud object storage.
    """
    if not 'index' in kwargs:
      raise("index must be provided")
    read_response = self._source.read_after(query=query, timestamp_str=after, binary_columns=binary_cols, media_id_col="id", dest_folder_col="id", **kwargs)
    # make sure only the featuretable columns are ingested
    cols = [e.name for e in ft.entities]
    cols.extend([f.name for f in ft.features])
    cols.extend([ft.created_col, ft.evt_col])
    if ignore_when_empty != None:
      read_response.dataframe = read_response.dataframe.dropna(subset=ignore_when_empty)
    self.ingest_response(ft, read_response, all_columns=cols)

  def read_all(self) -> pd.DataFrame:
    """
    Read all data from the source.
    """
    return self._source.read()

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
        limit: Optional[int] = None,
        only_most_recent: Optional[bool] = True,
        diff_table: Optional[str] = None,
        diff_join_key: Optional[str] = None,
        join_key: Optional[str] = None,
        diff_where: Optional[Dict] = None) -> pd.DataFrame:
    """ 
    Get the training features for the given feature view.
    
    args:
    
    - feature_table: Feature view name
    - features_selected: A list of features to be selected. If None, all features will be selected.
    - date_from: The start date of the training period. If None, the start date of the feature table will be used.
    - date_to: The end date of the training period. If None, the end date of the feature table will be used.
    - only_most_recent: If True, only the most recent features will be selected. If False, all features will be selected.
    - diff_table: If not None, the features will be compared with the features in the diff_table. The diff_table should have a column with the same name of the features table in order to compare. This is useful for when you want to filter, at query time, features from the result.
    - diff_join_key: The join key to be used to compare the features.
    - diff_where: A dictionary with the where clause to be used to compare the features.
    
    returns:
    
    A dataframe with the training features.
    """
    table_name = feature_table.name
    if features_selected is None:
      columns = "*"
    else:
      columns = ",".join(map(lambda x: f"{table_name}.{x}", features_selected))
    join = ""
    if diff_table and diff_join_key:
      left_join_key = join_key if join_key else diff_join_key
      join = f"LEFT JOIN {diff_table} ON {table_name}.{left_join_key} = {diff_table}.{diff_join_key}"
      if diff_where:
        for k,v in diff_where.items():
          join += f" AND {diff_table}.{k} = '{v}'"
    where = ""
    if date_from:
      where += f"WHERE {table_name}.created_timestamp >= '{date_from.isoformat()}'"
    if date_to:
      if where != "":
        where += f" AND {table_name}.created_timestamp <= '{date_to.isoformat()}'"
      else:
        where += f"WHERE {table_name}.created_timestamp <= '{date_to.isoformat()}'"
    if diff_table and where != "":
      where += f" AND {diff_table}.{diff_join_key} is null"
    elif diff_table:
      where += f"WHERE {diff_table}.{diff_join_key} is null"
    if limit:
      where += f" LIMIT {limit}"
    query = f"SELECT {columns} FROM {table_name} {join} {where}"
    df = self._sink.read_table(query)
    if only_most_recent:
      return df.sort_values(by="created_timestamp", ascending=False) \
        .groupby(by=[e.name for e in feature_table.entities]) \
        .tail(1)
    else:
      return df


  def ingest_schema(self, feature_table: FeatureTable, schema_file_path: str) -> None:
    """
    Ingest the schema of the feature table into the feature store. Useful when you're creating a new feature table.

    args:
    
    - feature_table: FeatureTable object
    - schema_file_path: Twhe local path to the schema file.

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
    # call get_view to force an apply of the schema
    feature_table.get_view()

  def get_sink_last_ts(self, feature_table: 'FeatureTable', date_from: Optional[datetime] = None, where: Optional[Dict[str, Any]] = None) -> Any:
    """
    Get the last row of the feature table.
    This is particularly useful when ingesting or reading data,
    the result of this method could be piped into the date_from
    of your method to read data from a source.

    args:
    
    - feature_table: FeatureTable object
    - date_from: The start date of the training period. If None, the start date of the feature table will be used.

    returns:

    - A timestamp of the same type used in the event_timestamp column
    """
    row = self._sink.get_last_row(feature_table, date_from=date_from, where=where)
    if row is None or row.empty:
      return None
    # rename column if the db returned in the format bellow, otherwise no-op
    row = row.rename(columns={f"{feature_table.evt_col}": "max"})
    return row["max"][0]

  def apply(self, objects: Union[feast.Entity, feast.FeatureView, feast.OnDemandFeatureView, feast.FeatureService,
    List[Union[feast.FeatureView, feast.OnDemandFeatureView, feast.Entity, feast.FeatureService]]],
        objects_to_delete: List[Union[feast.FeatureView, feast.OnDemandFeatureView, feast.Entity, feast.FeatureService, None]] = None,
        partial: bool = True):
    self._fs.apply(objects=objects, objects_to_delete=objects_to_delete, partial=partial)