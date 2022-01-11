import typing
import feast
from google.protobuf.duration_pb2 import Duration
import pandas as pd
from elemeno_ai_sdk.features.feature_store  import BaseFeatureStore

class FeatureTableDefinition:

    def __init__(self, name: str, event_column: str,
            feature_store: BaseFeatureStore,
            entities: typing.List[feast.Entity] = None,
            features: typing.List[feast.Feature] = None,
            duration_seconds=86400,
            online=False):
        self.name = name
        self.evt_col = event_column
        self._entities = [] if entities is None else entities
        self._features = [] if features is None else features
        self._feast_elm = feature_store
        self._duration = duration_seconds
        self._online = online
        
    
    @property
    def entities(self):
        return self._entities
    
    @entities.setter
    def entities(self, value):
        self._entities = value
    
    @property
    def features(self):
        return self._features
    
    @features.setter
    def features(self, value):
        self._features = value

    def register_entities(self, *entities: feast.Entity) -> None:
        self.entities.extend(list(entities))

    def register_features(self, *features: feast.Feature) -> None:
        self.features.extend(list(features))

    def register_entity(self, entity: feast.Entity) -> None:
        self.entities.append(entity)

    def register_feature(self, feature: feast.Feature) -> None:
        self.features.append(feature)

    def ingest(self, dataframe: pd.DataFrame):
        self._feast_elm.ingest(self._get_ft(), dataframe)


    def _get_ft(self):

        ft_source = feast.BigQuerySource(
            event_timestamp_column=self.evt_col,
            table_ref=self.name,
            created_timestamp_column="created_timestamp"
        )

        return feast.FeatureView(
            name = self.name,
            entities=self._entities,
            ttl=Duration(self._duration * 1),
            features=self._features,
            online=self._online,
            batch_source=ft_source,
            tags={}
        )
    
    def get_view(self) -> feast.FeatureView:
        f = self._get_ft()
        return f


