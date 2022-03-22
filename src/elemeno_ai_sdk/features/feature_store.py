import abc
import typing
import feast
from feast.registry import Registry
from feast.infra.offline_stores.bigquery import BigQueryOfflineStore, BigQueryOfflineStoreConfig
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.online_stores.redis import RedisOnlineStore, RedisOnlineStoreConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from google.cloud import bigquery
import logging
from elemeno_ai_sdk.features.utils import create_insert_into
from elemeno_ai_sdk.config import Configs

class BaseFeatureStore(metaclass=abc.ABCMeta):

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'ingest') and
            callable(subclass.ingest) and
            hasattr(subclass, 'ingest_from_query') and
            callable(subclass.ingest_rs) and
            hasattr(subclass, 'ingest_rs') and
            callable(subclass.ingest_from_query) and
            hasattr(subclass, 'get_historical_features') and
            callable(subclass.get_historical_features) and
            hasattr(subclass, 'get_online_features') and
            callable(subclass.get_online_features) and
            hasattr(subclass, 'apply') and
            callable(subclass.apply) and
            hasattr(subclass, 'config') and
            hasattr(subclass, 'fs'))

    def ingest(self, ft: feast.FeatureView, df: pd.DataFrame):
        pass

    def ingest_rs(self, ft: feast.FeatureView, df: pd.DataFrame, conn_str: str, 
        expected_columns: typing.List[str], created_timestamp_name: str):
        pass

    def ingest_from_query(self, ft: feast.FeatureView, query: str):
        pass

    def get_historical_features(self, entity_source: pd.DataFrame, feature_refs: typing.List[str]) -> RetrievalJob:
        pass

    def get_online_features(self, entities: typing.List[typing.Dict[str, typing.Any]],
            requested_features: typing.Optional[typing.List[str]]=None) \
            -> feast.online_response.OnlineResponse:
        pass

    def apply(self, objects: typing.Union[feast.Entity, feast.FeatureView, feast.OnDemandFeatureView, feast.FeatureService,
        typing.List[typing.Union[feast.FeatureView, feast.OnDemandFeatureView, feast.Entity, feast.FeatureService]]],
            objects_to_delete: typing.List[typing.Union[feast.FeatureView, feast.OnDemandFeatureView, feast.Entity, feast.FeatureService, None]] = None,
            partial: bool = True) -> None:
        pass

    fs: feast.FeatureStore

BaseFeatureStore.register
class FeatureStore:
    def __init__(self) -> None:
        """
        FeatureStore is a BigQuery compatible Feature Store implementation
        """
        logging.basicConfig()
        logging.getLogger().setLevel("INFO")
        self._elm_config = Configs.instance()
        self._fs = feast.FeatureStore(repo_path=self._elm_config.feature_store.feast_config_path)
        self.config = self._fs.config

    @property
    def fs(self) -> feast.FeatureStore:
        return self._fs
    
    def _parse_table_name_from_offline_config(self, offline_store, name) -> str:
        table_name = ""
        if hasattr(self._fs.config.offline_store, 'project_id'):
            table_name += f"{self._fs.config.offline_store.project_id}."
        if hasattr(self._fs.config.offline_store, 'dataset'):
            table_name += f"{self._fs.config.offline_store.dataset}."
        table_name += name
        return table_name

    def ingest(self, ft: feast.FeatureView, df: pd.DataFrame, schema: typing.List[typing.Dict] = None):
        table_name = self._parse_table_name_from_offline_config(self._fs.config.offline_store, ft.name)
        project_id = self._fs.config.offline_store.project_id
        location = self._fs.config.offline_store.location
        df.to_gbq(destination_table=table_name,
            project_id=project_id, if_exists="append", location=location, table_schema=schema)

    def ingest_from_query(self, ft: feast.FeatureView, query: str):
      table_name = self._parse_table_name_from_offline_config(self._fs.config.offline_store, ft.name)
      project_id = self._fs.config.offline_store.project_id
      client = bigquery.Client(project=project_id)
      final_query = create_insert_into(table_name, query)
      logging.info("Will perform query: {}".format(final_query))
      client.query(final_query).result()

    def ingest_rs(self, ft: feast.FeatureView, df: pd.DataFrame, 
        conn_str: str, expected_columns: typing.List[str],
        created_timestamp_name: str):
      df = self._with_ts_if_not_present(ft, df, created_timestamp_name)
      df = df.filter(expected_columns, axis=1)
      conn = create_engine(conn_str, isolation_level="AUTOCOMMIT")
      try:
        df.to_sql(f"{ft.name}",
                conn, index=False, if_exists='append', method='multi', chunksize=2000)
      finally:
        conn.dispose()


    def _with_ts_if_not_present(self, ftb: feast.FeatureView, df: pd.DataFrame, created_timestamp: str) -> pd.DataFrame:
        if (not created_timestamp in df or len(df[created_timestamp].isna()) == len(df)):
            df[created_timestamp] = pd.to_datetime('now', utc=True)
        return df

    def get_historical_features(self, entity_source: pd.DataFrame, feature_refs: typing.List[str]) -> RetrievalJob:
        return self._fs.get_historical_features(entity_source, feature_refs)

    def get_online_features(self, entities: typing.List[typing.Dict[str, typing.Any]],
            requested_features: typing.Optional[typing.List[str]]=None) \
            -> feast.online_response.OnlineResponse:
        if self._fs.config.online_store == None:
            raise ValueError("Online store is not configure, make sure to configure the property online_store in the config yaml")
        return self._fs.get_online_features(features=requested_features, entity_rows=entities)

    def apply(self, objects: typing.Union[feast.Entity, feast.FeatureView, feast.OnDemandFeatureView, feast.FeatureService,
        typing.List[typing.Union[feast.FeatureView, feast.OnDemandFeatureView, feast.Entity, feast.FeatureService]]],
            objects_to_delete: typing.List[typing.Union[feast.FeatureView, feast.OnDemandFeatureView, feast.Entity, feast.FeatureService, None]] = None,
            partial: bool = True):
        self._fs.apply(objects=objects, objects_to_delete=objects_to_delete, partial=partial)




