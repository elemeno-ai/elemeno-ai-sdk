import typing
from feast.protos.feast.types.EntityKey_pb2 import EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.type_map import feast_value_type_to_python_type
import pandas as pd
from datetime import datetime
from elemeno_ai_sdk.features.feature_store import BaseFeatureStore
from elemeno_ai_sdk.features.feature_table import FeatureTableDefinition

class Query:
    
    def __init__(self, feature_store: BaseFeatureStore, 
                 definition: FeatureTableDefinition):
        self._feature_store = feature_store
        self._definition = definition
        
    def get_historical_features(self, entities_where: pd.DataFrame) -> pd.DataFrame:
        ft = self._definition
        entities = ft.entities
        cols = [p.name for p in entities]
        source_entity = pd.DataFrame(columns=cols)
        input_cols = entities_where.columns
        for c in cols:
            if c not in input_cols:
                raise ValueError(f"Invalid input. The column {c} is missing from the where object")
            source_entity[c] = entities_where[c]
        if not ft.evt_col in input_cols:
            raise ValueError("Missing the event timestamp column in input")
        source_entity[ft.evt_col] = entities_where[ft.evt_col]
        features = [f"{ft.name}:{fd.name}" for fd in ft.features]
        job = self._feature_store.get_historical_features(feature_refs=features, entity_source=source_entity)
        return job.to_df()
    
    def get_online_features(self, entity_keys: typing.List[typing.Any], features_requested: typing.Optional[typing.List[str]]=None) \
        -> typing.List[typing.Tuple[typing.Optional[datetime], typing.Optional[typing.Dict[str, ValueProto]]]]:
        k = EntityKeyProto()
        entity_keys_vals = [
            {
                k: feast_value_type_to_python_type(v)
            }
            for v in entity_keys
        ]
        return self._feature_store.get_online_features(self._definition.get_view(), entity_keys_vals, features_requested)