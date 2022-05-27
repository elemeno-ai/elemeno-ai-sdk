import typing

import pandas as pd
from google.cloud import bigquery
from elemeno_ai_sdk import logger
from elemeno_ai_sdk.ml.features.feature_store import FeatureStore
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable
from elemeno_ai_sdk.ml.features.utils import create_insert_into
from .base_ingestion import Ingestion

class BigQueryIngestion(Ingestion):

  def __init__(self, fs: FeatureStore):
    super().__init__(fs)
    self._fs = fs

  def ingest(self, to_ingest: pd.DataFrame, ft: FeatureTable, expected_columns: typing.List[str] = [], schema: typing.List[typing.Dict] = None) -> None:
    if len(expected_columns) == 0:
      logger.warning("No expected columns provided. Will ingest all columns.")
      expected_columns = to_ingest.columns.to_list()
    table_name = self._parse_table_name_from_offline_config(ft.name)
    project_id = self._fs.config.offline_store.project_id
    location = self._fs.config.offline_store.location

    to_ingest = super().with_ts_if_not_present(to_ingest, ft.created_col)
    if ft.created_col not in expected_columns:
      expected_columns.append(ft.created_col)
    to_ingest = to_ingest.filter(expected_columns, axis=1)
    to_ingest.to_gbq(destination_table=table_name,
        project_id=project_id, if_exists="append", location=location, table_schema=schema)

  def ingest_from_query(self, query: str, ft: FeatureTable) -> None:
    table_name = self._parse_table_name_from_offline_config(ft.name)
    project_id = self._fs.config.offline_store.project_id
    client = bigquery.Client(project=project_id)
    final_query = create_insert_into(table_name, query)
    logger.info("Will perform query: %s", final_query)
    client.query(final_query).result()

  def _parse_table_name_from_offline_config(self, name) -> str:
    table_name = ""
    if hasattr(self._fs.config.offline_store, 'project_id'):
      table_name += f"{self._fs.config.offline_store.project_id}."
    if hasattr(self._fs.config.offline_store, 'dataset'):
      table_name += f"{self._fs.config.offline_store.dataset}."
    table_name += name
    return table_name
