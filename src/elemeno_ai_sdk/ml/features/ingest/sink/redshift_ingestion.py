from datetime import datetime
import typing
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import sqlalchemy
import pandas as pd

from elemeno_ai_sdk.ml.features.feature_table import FeatureTable
from elemeno_ai_sdk import logger
from elemeno_ai_sdk.ml.features.ingest.sink.base_ingestion import Ingestion

MAP_TYPES = {
  ''
}
class RedshiftIngestion(Ingestion):

  def __init__(self, fs, connection_string: str):
    super().__init__()
    self._conn_str = connection_string

  def read_table(self, query: str) -> pd.DataFrame:
    engine = create_engine(self._conn_str, hide_parameters=True, isolation_level="READ COMMITTED")
    with engine.connect().execution_options(autocommit=True) as conn:
      return pd.read_sql(query, con = conn)

  def ingest(self, to_ingest: pd.DataFrame, ft: FeatureTable, renames: typing.Optional[typing.Dict[str, str]] = None,
      expected_columns: typing.Optional[typing.List[str]] = None) -> None:
    if renames is not None:
      to_ingest = to_ingest.rename(columns=renames)
    if expected_columns is None or len(expected_columns) == 0:
      logger.warning("No expected columns provided. Will ingest all columns.")
      expected_columns = to_ingest.columns.to_list()
    to_ingest = to_ingest.filter(expected_columns, axis=1)
    conn = create_engine(self._conn_str, hide_parameters=True, isolation_level="AUTOCOMMIT")
    try:
      logger.info("Within RedshiftIngestion.ingest, about to create table {}".format(ft.name))
      # create table if not exists
      to_ingest = self.create_table(to_ingest, ft, conn)
      # ingest data
      max_rows_per_insert = 5000
      insert_pages = len(to_ingest) // max_rows_per_insert + 1
      for i in range(insert_pages):
        logger.info("Ingesting page %d of %d", i, insert_pages)
        if len(to_ingest) < (i+1) * max_rows_per_insert:
          chunk = to_ingest.iloc[i*max_rows_per_insert:]
        else:
          chunk = to_ingest.iloc[i * max_rows_per_insert:(i + 1) * max_rows_per_insert]
        chunk.to_sql(f"{ft.name}",
                conn, index=False, if_exists='append',
                method='multi', 
                chunksize=1000)
    except Exception as exception:
      logger.error("Failed to ingest data to Redshift: %e", exception)
      raise exception
    finally:
      conn.dispose()
  
  def create_table(self, to_ingest: pd.DataFrame, ft: FeatureTable, engine: sqlalchemy.engine.Engine) -> pd.DataFrame:
    to_ingest = to_ingest.convert_dtypes()
    # FIXME: this is a hack to get around the fact we're not using FeatureTable here
    date_cols = ["create_timestamp", "event_timestamp", "created_date", "record_date", "updated_date"]
    columns = {}
    for col, dtype in to_ingest.dtypes.items():
      if col in date_cols:
        columns[col] = "TIMESTAMP"
      elif dtype == "object":
        columns[col] = "SUPER"
        to_ingest[col] = to_ingest[col].astype("str")
      elif dtype == "string":
        columns[col] = "VARCHAR(12600)"
      elif dtype == "Int64":
        columns[col] = "BIGINT"
      elif dtype == "Float64":
        columns[col] = "DECIMAL"
      elif dtype == "bool":
        columns[col] = "BOOLEAN"
      else:
        columns[col] = "VARCHAR(12600)"
    create = "CREATE TABLE IF NOT EXISTS {} (".format(ft.name)
    for col, dtype in columns.items():
      create += "{} {},".format(col, dtype)
    create = create[:-1] + ")"
    engine.execute(create)
    return to_ingest

  def ingest_from_query(self, query: str, ft: FeatureTable, expected_columns: typing.List[str]) -> None:
    raise NotImplementedError("RedshiftIngestion.ingest_from_query is not implemented")