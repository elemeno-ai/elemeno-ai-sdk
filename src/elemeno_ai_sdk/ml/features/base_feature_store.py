import abc
from datetime import datetime
import typing
import feast
import pandas as pd
from feast.infra.offline_stores.offline_store import RetrievalJob

class BaseFeatureStore(metaclass=abc.ABCMeta):

  @classmethod
  def __subclasshook__(cls, subclass):
    return (hasattr(subclass, 'ingest') and
        callable(subclass.ingest) and
        hasattr(subclass, 'ingest_from_query') and
        callable(subclass.ingest_from_query) and
        hasattr(subclass, 'get_historical_features') and
        callable(subclass.get_historical_features) and
        hasattr(subclass, 'get_online_features') and
        callable(subclass.get_online_features) and
        hasattr(subclass, 'apply') and
        callable(subclass.apply) and
        hasattr(subclass, 'config') and
        hasattr(subclass, 'fs'))

  @abc.abstractmethod
  def ingest(self, ft: feast.FeatureView, df: pd.DataFrame):
    pass

  @abc.abstractmethod
  def ingest_from_query_same_source(self, ft: 'FeatureTable', query: str):
    pass
  
  @abc.abstractmethod
  def read_and_ingest_from_query(self, ft: 'FeatureTable', query: str, **kwargs):
    pass
  
  @abc.abstractmethod
  def read_and_ingest_from_query_after(self, ft: 'FeatureTable', query: str, after: str, **kwargs):
    pass

  @abc.abstractmethod
  def get_historical_features(self, entity_source: pd.DataFrame, feature_refs: typing.List[str]) -> RetrievalJob:
    pass

  @abc.abstractmethod
  def get_training_features(self, feature_table: 'FeatureTable',
        features_selected: typing.List[str] = None,
        from_: typing.Optional[datetime] = None,
        to_: typing.Optional[datetime] = None,
        limit: typing.Optional[int] = None,
        only_most_recent: typing.Optional[bool] = True) -> pd.DataFrame:
    pass

  @abc.abstractmethod
  def get_online_features(self, entities: typing.List[typing.Dict[str, typing.Any]],
        requested_features: typing.Optional[typing.List[str]]=None) \
        -> feast.online_response.OnlineResponse:
    pass
  
  @abc.abstractmethod
  def get_sink_last_ts(self, feature_table: 'FeatureTable') -> pd.DataFrame:
    pass
  
  @abc.abstractmethod
  def apply(self, objects: typing.Union[feast.Entity, feast.FeatureView, feast.OnDemandFeatureView, feast.FeatureService,
    typing.List[typing.Union[feast.FeatureView, feast.OnDemandFeatureView, feast.Entity, feast.FeatureService]]],
        objects_to_delete: typing.List[typing.Union[feast.FeatureView, feast.OnDemandFeatureView, feast.Entity, feast.FeatureService, None]] = None,
        partial: bool = True) -> None:
    pass

  fs: feast.FeatureStore