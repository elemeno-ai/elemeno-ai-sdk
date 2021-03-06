import typing
from google.protobuf.duration_pb2 import Duration
import pandas as pd
import feast
from elemeno_ai_sdk import logger
from elemeno_ai_sdk.ml.features.types import FeatureType

class FeatureTable:
  """ A FeatureTable is the object that is used to define feature tables on Elemeno feature store.

  If you're looking to create a new feature table or read data look at ingest_schema of the class FeatureStore.
  """

  def __init__(self, name: str, feature_store: feast.FeatureStore,
          entities: typing.List[feast.Entity] = None,
          features: typing.List[feast.Feature] = None,
          ttl_duration_weeks=52,
          online=False, event_column: str = "event_timestamp",
          created_column: str = "created_timestamp"):
    self.name = name
    self._entities = [] if entities is None else entities
    self._features = [] if features is None else features
    self._feast_elm = feature_store
    self._duration = ttl_duration_weeks
    self._online = online
    self._evt_col = event_column
    self._created_col = created_column
    self._table_schema = []


  @property
  def entities(self):
    return self._entities

  @property
  def evt_col(self):
    return self._evt_col

  @property
  def created_col(self):
    return self._created_col
  
  @property
  def table_schema(self) -> typing.List[typing.Dict]:
    return self._table_schema

  @entities.setter
  def entities(self, value):
    self._entities = value


  @property
  def features(self):
    return self._features

  @features.setter
  def features(self, value):
      self._features = value

  def set_table_schema(self, value: typing.List[typing.Dict]) -> None:
    self._table_schema = value
  
  def register_entities(self, *entities: feast.Entity) -> None:
      self.entities.extend(list(entities))

  def register_features(self, *features: feast.Feature) -> None:
      self.features.extend(list(features))

  def register_entity(self, entity: feast.Entity) -> None:
      self.entities.append(entity)

  def register_feature(self, feature: feast.Feature) -> None:
      self.features.append(feature)
    
  def all_columns(self) -> typing.List[str]:
    cols = []
    for e in self._entities:
      cols.append(e.name)
    for f in self._features:
      cols.append(f.name)
    cols.append(self._evt_col)
    cols.append(self._created_col)
    return cols

  def _get_ft(self):
    dataset = self._feast_elm.config.offline_store.dataset
    ft_source = feast.BigQuerySource(
        table_ref=f"{dataset}.{self.name}",
        event_timestamp_column=self.evt_col,
        created_timestamp_column=self.created_col
    )

    fv = feast.FeatureView(
        name = self.name,
        entities=[e.name for e in self._entities],
        ttl=Duration(seconds=self._duration * 604800),
        features=self._features,
        online=self._online,
        batch_source=ft_source,
        tags={}
    )
    self._feast_elm.apply(objects=self.entities)
    self._feast_elm.apply(objects=fv)
    return fv

  def _get_ft_rs(self):
    ft_source = feast.RedshiftSource(
      table= f"{self.name}",
      event_timestamp_column=self.evt_col,
      created_timestamp_column=self.created_col
    )

    fv = feast.FeatureView(
      name = self.name,
      entities=[e.name for e in self._entities],
      ttl=Duration(seconds=self._duration * 604800),
      features=self._features,
      online=self._online,
      batch_source=ft_source,
      tags={}
    )
    self._feast_elm.apply(objects=self.entities)
    self._feast_elm.apply(objects=fv)

    return fv

  def _get_ft_file(self):
    ft_source = feast.FileSource(
      path=f"data/{self.name}",
      event_timestamp_column=self.evt_col,
      created_timestamp_column=self.created_col
    )

    fv = feast.FeatureView(
      name = self.name,
      entities=[e.name for e in self._entities],
      ttl=Duration(seconds=self._duration * 604800),
      features=self._features,
      online=self._online,
      batch_source=ft_source,
      tags={}
    )
    self._feast_elm.apply(objects=self.entities)
    self._feast_elm.apply(objects=fv)

    return fv

  def get_view(self) -> feast.FeatureView:
    offline_store_type = type(self._feast_elm.fs.config.offline_store).__name__
    if (offline_store_type == "BigQueryOfflineStoreConfig"):
      f = self._get_ft()
    elif (offline_store_type == "RedshiftOfflineStoreConfig"):
      f = self._get_ft_rs()
    elif (offline_store_type == "FileOfflineStoreConfig"):
      f = self._get_ft_file()
    else:
      print(offline_store_type)
      raise ValueError("Unsupported offline store type")
    return f
