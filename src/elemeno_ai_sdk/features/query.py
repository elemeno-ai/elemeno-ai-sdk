import typing
import pandas as pd
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
        job = self._feature_store.get_historical_features(entity_source=source_entity, feature_refs=features)
        return job.to_df()
    
    def get_online_features(self, entity_keys: typing.List[typing.Dict[str, typing.Any]]) \
        -> pd.DataFrame:
        ft_name = self._definition.name
        features_requested = []
        for f in self._definition.features:
            features_requested.append(f"{ft_name}:{f}")
        
        ents = [e.name for e in self._definition.entities]

        for t in entity_keys:
            for k, _ in t.items():
                if k not in ents:
                    raise ValueError(f"The key {k} is invalid, it's not part of the entities in the feature table definition")

        return self._feature_store.get_online_features(requested_features=features_requested, entities=entity_keys)