import typing
from sqlalchemy import create_engine
import pandas as pd

from elemeno_ai_sdk.ml.features.feature_store import FeatureStore
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable
from elemeno_ai_sdk import logger

from .base_ingestion import Ingestion

class RedshiftIngestion(Ingestion):

  def __init__(self, fs: FeatureStore, connection_string: str):
    super().__init__(fs)
    self._conn_str = connection_string

  def ingest(self, to_ingest: pd.DataFrame, ft: FeatureTable, expected_columns: typing.List[str] = []) -> None:
    to_ingest = to_ingest.filter(expected_columns, axis=1)
    if len(expected_columns) == 0:
      logger.warning("No expected columns provided. Will ingest all columns.")
      expected_columns = to_ingest.columns.to_list()
    conn = create_engine(self._conn_str, isolation_level="AUTOCOMMIT")
    try:
      to_ingest.to_sql(f"{ft.name}",
              conn, index=False, if_exists='append', method='multi', chunksize=2000)
    except Exception as exception:
      logger.error("Failed to ingest data to Redshift: %e", exception)
      raise exception
    finally:
      conn.dispose()
  
  def ingest_from_query(self, query: str, ft: FeatureTable, expected_columns: typing.List[str]) -> None:
    raise NotImplementedError("RedshiftIngestion.ingest_from_query is not implemented")