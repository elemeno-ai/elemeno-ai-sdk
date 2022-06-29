from datetime import datetime
import typing
import pandas as pd
from typing import Optional
from elemeno_ai_sdk.ml.features.feature_store import BaseFeatureStore
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable
import logging

logger = logging.getLogger("FeatureQuery")

class Query:

    def __init__(self, feature_store: BaseFeatureStore,
                 definition: FeatureTable):
        """ This class is used to query features from the feature store.
        It's not necessary to use this class directly, it's used by the FeatureStore class.

        args:
          - feature_store: The feature store object.
          - definition: The feature table definition.
        """
        self._feature_store = feature_store
        self._definition = definition

    def get_historical_features(self,
                                entities_where: pd.DataFrame,
                                features_selected: typing.List[str] = None) -> pd.DataFrame:
        """ Queries the feature store for historical features.
        This method requires you to specify a dataframe with the keys of the entities you want to query.
        You may also specify a list of features you want to query.
        If you don't specify a list of features, all features will be queried.

        args:
          - entities_where: A dataframe with the keys of the entities you want to query.
          - features_selected: A list of features you want to query.
        returns:
          - A dataframe with the features.
        """
        ft = self._definition
        logging.info(ft.name)
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
        # features = []
        if features_selected is None:
          features = [f"{ft.name}:{fd.name}" for fd in ft.features]
        else:
          for f_selected in features_selected:
            if f_selected not in ft.features:
              raise ValueError(f"Invalid input. The selected {f_selected} is not present in the Feature View.")
            else:
              print(f"{f_selected} is OK!")
          features = [f"{ft.name}:{fcustom}" for fcustom in features_selected]
        job = self._feature_store.get_historical_features(entity_source=source_entity, feature_refs=features)
        return job.to_df()

    def get_online_features(self, entity_keys: typing.List[typing.Dict[str, typing.Any]]) \
        -> pd.DataFrame:
        """ Queries the feature store for online features.
        This method requires you to specify a list of dictionaries with the keys of the entities you want to query.
        By default all features will be returned

        args:
          - entity_keys: A list of dictionaries with the keys of the entities you want to query.
        returns:
          - A dataframe with the features.
        """
        ft_name = self._definition.name
        features_requested = []
        for f in self._definition.features:
            features_requested.append(f"{ft_name}:{f.name}")

        ents = [e.name for e in self._definition.entities]

        for t in entity_keys:
            for k, _ in t.items():
                if k not in ents:
                    raise ValueError(f"The key {k} is invalid, it's not part of the entities in the feature table definition")

        return self._feature_store.get_online_features(requested_features=features_requested, entities=entity_keys)
