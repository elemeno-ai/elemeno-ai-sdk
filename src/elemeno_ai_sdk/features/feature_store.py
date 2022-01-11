import abc
import typing
import feast
from feast.infra.offline_stores.bigquery import BigQueryOfflineStore, BigQueryOfflineStoreConfig
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.online_stores.redis import RedisOnlineStore, RedisOnlineStoreConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
import pandas as pd
from datetime import datetime

class BaseFeatureStore(metaclass=abc.ABCMeta):

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'ingest') and
            callable(subclass.ingest) and
            hasattr(subclass, 'get_historical_features') and
            callable(subclass.get_historical_features) and
            hasattr(subclass, 'get_online_features') and
            callable(subclass.get_online_features))
    
    def ingest(self, ft: feast.FeatureView, df: pd.DataFrame):
        pass

    def get_historical_features(self, feature_views: typing.List[feast.FeatureView], 
            feature_refs: typing.List[str], entity_source: pd.DataFrame) -> RetrievalJob:
        pass
    
    def get_online_features(self, feature_view: feast.FeatureView,
        entity_keys: typing.List[EntityKeyProto], requested_features: typing.Optional[typing.List[str]]=None) \
            -> typing.List[typing.Tuple[typing.Optional[datetime], typing.Optional[typing.Dict[str, ValueProto]]]]:
        pass

BaseFeatureStore.register
class FeatureStoreBQ: 
    def __init__(self, 
        bigquery_offline: BigQueryOfflineStore,
        config: BigQueryOfflineStoreConfig,
        registry: feast.Registry,
        redis_online: typing.Optional[RedisOnlineStore]=None,
        redis_online_config: typing.Optional[RedisOnlineStoreConfig]=None) -> None:
        """
        FeatureStore is a BigQuery compatible Feature Store implementation
        """
        self.store = bigquery_offline
        self.config = config
        self.registry = registry
        self.online_store = redis_online
        self.online_config = redis_online_config
    
    def ingest(self, ft: feast.FeatureView, df: pd.DataFrame):
        project_id = self.config.project_id
        dataset = self.config.dataset
        location = self.config.location
        df.to_gbq(destination_table=f"{dataset}.{ft.name}", 
            project_id=project_id, if_exists="append", location=location)
        
    def get_historical_features(self, feature_views: typing.List[feast.FeatureView], 
            feature_refs: typing.List[str], entity_source: pd.DataFrame) -> RetrievalJob:
        return self.store.get_historical_features(self.config, feature_views,
            feature_refs, entity_source, self.registry, project=self.config.project_id)
    
    def get_online_features(self, feature_view: feast.FeatureView,
        entity_keys: typing.List[EntityKeyProto], requested_features: typing.Optional[typing.List[str]]=None) \
            -> typing.List[typing.Tuple[typing.Optional[datetime], typing.Optional[typing.Dict[str, ValueProto]]]]:
        if self.online_store == None:
            raise ValueError("Online store is not configure, make sure to inform redis_online parameter to this class")
        return self.online_store.online_read(self.online_config, feature_view, 
            entity_keys, requested_features)

        
        
