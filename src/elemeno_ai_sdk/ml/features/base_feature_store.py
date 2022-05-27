import abc
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