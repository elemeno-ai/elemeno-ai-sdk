import pandas as pd
import logging
from elemeno_ai_sdk.features.feature_store import BaseFeatureStore
from elemeno_ai_sdk.features.feature_table import FeatureTableDefinition
from sql_metadata import Parser

logger = logging.getLogger("FeatureIngestion")

class FeatureIngestion:

    def __init__(self,
            feature_store: BaseFeatureStore,
            feature_table: FeatureTableDefinition) -> None:

        self._feature_store = feature_store
        self._feature_table = feature_table

    def ingest(self, to_ingest: pd.DataFrame) -> None:
        expected_columns = self._feature_table.all_columns()

        try:
            to_ingest = to_ingest.filter(expected_columns, axis=1)
            for tv in to_ingest.columns:
                expected_columns.remove(tv)
            if len(expected_columns) > 0:
                raise ValueError(f"""There are missing columns in the dataframe trying to be ingested.
                    Check the schema of the feature_table. Missing are {expected_columns} """)
            self._feature_store.ingest(self._feature_table.get_view(), to_ingest, schema=self._feature_table.table_schema)
        except e:
            logger.error("There's some column in the df trying to be ingest not available in the features", e)
            raise e

    def ingest_from_query(self, query: str) -> None:
        """
        Requirements for validation:
          - All columns must be aliased with same name from feature store
          - Any statements before select must end with ;
          - Select statement should not end with ;
        """
        expected_columns = []
        for e in self._feature_table.entities:
          expected_columns.append(e.name)
        for f in self._feature_table.features:
          expected_columns.append(f)

        try:
          for tv in Parser(query.split(";")[-1]).columns_aliases_names:
            expected_columns.remove(tv)
          if len(expected_columns) > 0:
            raise ValueError(f"""There are missing columns in the select query trying to be ingested.
                      Check the schema of the feature_table. Missing are {expected_columns} """)
          self._feature_store.ingest_from_query(self._feature_table.get_view(), query, schema=self._feature_table.table_schema)
        except e:
          logger.error("There's some column in the df trying to be ingest not available in the features", e)
          raise e
