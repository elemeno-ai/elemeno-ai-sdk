import typing
import feast
import json
from google.protobuf.duration_pb2 import Duration
import pandas as pd
from elemeno_ai_sdk.features.feature_store  import BaseFeatureStore
from elemeno_ai_sdk.features.types import FeatureType

class FeatureTableDefinition:

    def __init__(self, name: str, feature_store: BaseFeatureStore,
            entities: typing.List[feast.Entity] = None,
            features: typing.List[feast.Feature] = None,
            duration_seconds=86400,
            online=False, event_column: str = "event_timestamp", 
            created_column: str = "created_timestamp"):
        self.name = name
        self._entities = [] if entities is None else entities
        self._features = [] if features is None else features
        self._feast_elm = feature_store
        self._duration = duration_seconds
        self._online = online
        self._evt_col = event_column
        self._created_col = created_column
        
    
    @property
    def entities(self):
        return self._entities

    @property
    def evt_col(self):
        return self._evt_col

    @property
    def created_col(self):
        return self._created_col
    
    @entities.setter
    def entities(self, value):
        self._entities = value

    def ingest_schema(self, schema_file_path: str) -> None:
        """
        This method should be called if you want to use a jsonschema file to create the feature table
        If other entities/features were registered, this method will append the ones in the jsonschema to them

        Arguments:
        schema_file_path: str - The local path to the file containing the jsonschema definition

        """
        with open(schema_file_path, mode="r") as schema_file:
            jschema = json.loads(schema_file.read())
            table_schema = []
            for name, prop in jschema["properties"].items():
                fmt = prop["format"] if "format" in prop else None
                table_schema.append({"name": name, "type": FeatureType.from_str_to_bq_type(prop["type"], format=fmt).name})
                if "isKey" in prop and prop["isKey"] == "true":
                    self.register_entity(feast.Entity(name=name, description=name, value_type=FeatureType.from_str_to_feature_type(prop["type"])))
                else:
                    if "format" in prop and prop["format"] == "date-time":
                        continue
                    self.register_features(feast.Feature(name, FeatureType.from_str_to_feature_type(prop["type"])))
    
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
            table_ref=self.name,
            event_timestamp_column=self.evt_col,
            created_timestamp_column=self.created_col
        )

        return feast.FeatureView(
            name = self.name,
            entities=self._entities,
            ttl=Duration(seconds=self._duration * 1),
            features=self._features,
            online=self._online,
            batch_source=ft_source,
            tags={}
        )
    
    def get_view(self) -> feast.FeatureView:
        f = self._get_ft()
        return f


