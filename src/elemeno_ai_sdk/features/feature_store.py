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

    def get_historical_features(self, entity_source: pd.DataFrame, feature_refs: typing.List[str]) -> RetrievalJob:
        pass
    
    def get_online_features(self, entities: typing.List[typing.Dict[str, typing.Any]], 
            requested_features: typing.Optional[typing.List[str]]=None) \
            -> feast.online_response.OnlineResponse:
        pass

BaseFeatureStore.register
class FeatureStore: 
    def __init__(self) -> None:
        """
        FeatureStore is a BigQuery compatible Feature Store implementation
        """
        self.fs = feast.FeatureStore(repo_path=".")
    
    def ingest(self, ft: feast.FeatureView, df: pd.DataFrame):
        project_id = self.fs.config.offline_store.project_id
        dataset = self.fs.config.offline_store.dataset
        location = self.fs.config.offline_store.location
        df.to_gbq(destination_table=f"{dataset}.{ft.name}", 
            project_id=project_id, if_exists="append", location=location)
        
    def get_historical_features(self, entity_source: pd.DataFrame, feature_refs: typing.List[str]) -> RetrievalJob:
        return self.fs.get_historical_features(entity_source, feature_refs)
    
    def get_online_features(self, entities: typing.List[typing.Dict[str, typing.Any]], 
            requested_features: typing.Optional[typing.List[str]]=None) \
            -> feast.online_response.OnlineResponse:
        if self.fs.config.online_store == None:
            raise ValueError("Online store is not configure, make sure to configure the property online_store in the config yaml")
        return self.fs.get_online_features(features=requested_features, entity_rows=entities)
        
        
