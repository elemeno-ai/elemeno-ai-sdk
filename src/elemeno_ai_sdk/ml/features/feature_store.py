

import typing
import pandas as pd
import feast
from feast.infra.offline_stores.offline_store import RetrievalJob
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable
from elemeno_ai_sdk.ml.features.base_feature_store import BaseFeatureStore
from elemeno_ai_sdk.ml.features.ingest.source.elastic import ElasticIngestion
from elemeno_ai_sdk.config import Configs
from elemeno_ai_sdk.ml.features.ingest.sink.ingestion_sink_builder \
   import IngestionSinkBuilder, IngestionSinkType

class FeatureStore(BaseFeatureStore):
  def __init__(self, sink_type: IngestionSinkType, **kwargs) -> None:
    """
    FeatureStore is a BigQuery compatible Feature Store implementation
    """
    self._elm_config = Configs.instance()
    self._fs = feast.FeatureStore(repo_path=self._elm_config.feature_store.feast_config_path)
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
      to_ingest: pd.DataFrame, schema: typing.List[typing.Dict] = None):
    all_columns = to_ingest.columns.to_list()
    self._sink.ingest(to_ingest, feature_table, all_columns)

  def ingest_from_query(self, ft: FeatureTable, query: str):
    self._sink.ingest_from_query(query, ft)

  def ingest_from_elastic(self, feature_table: FeatureTable, index: str,
      query: str, host: str, username: str, password: str):
    elastic_source = ElasticIngestion(host=host, username=username, password=password)
    to_insert = elastic_source.read(index=index, query=query)
    all_columns = to_insert.columns.tolist()
    self._sink.ingest(to_insert, feature_table, all_columns)

  def get_historical_features(self, entity_source: pd.DataFrame, feature_refs: typing.List[str]) -> RetrievalJob:
    return self._fs.get_historical_features(entity_source, feature_refs)

  def get_online_features(self, entities: typing.List[typing.Dict[str, typing.Any]],
        requested_features: typing.Optional[typing.List[str]]=None) \
        -> feast.online_response.OnlineResponse:
    if self._fs.config.online_store is None:
      raise ValueError("Online store is not configure, make sure to configure the property online_store in the config yaml")
    return self._fs.get_online_features(features=requested_features, entity_rows=entities)

  def apply(self, objects: typing.Union[feast.Entity, feast.FeatureView, feast.OnDemandFeatureView, feast.FeatureService,
    typing.List[typing.Union[feast.FeatureView, feast.OnDemandFeatureView, feast.Entity, feast.FeatureService]]],
        objects_to_delete: typing.List[typing.Union[feast.FeatureView, feast.OnDemandFeatureView, feast.Entity, feast.FeatureService, None]] = None,
        partial: bool = True):
    self._fs.apply(objects=objects, objects_to_delete=objects_to_delete, partial=partial)
