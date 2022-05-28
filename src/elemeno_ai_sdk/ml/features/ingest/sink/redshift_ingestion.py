import typing
from sqlalchemy import create_engine
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

  def ingest(self, to_ingest: pd.DataFrame, ft: FeatureTable, expected_columns: typing.List[str] = []) -> None:
    to_ingest = to_ingest.filter(expected_columns, axis=1)
    if len(expected_columns) == 0:
      logger.warning("No expected columns provided. Will ingest all columns.")
      expected_columns = to_ingest.columns.to_list()
    conn = create_engine(self._conn_str, isolation_level="AUTOCOMMIT")
    try:
      # create table if not exists
      to_ingest = self.create_table(to_ingest, ft, conn)
      # ingest data
      to_ingest.to_sql(f"{ft.name}",
              conn, index=False, if_exists='append', method='multi', chunksize=2000)
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
        columns[col] = "VARCHAR(4096)"
      elif dtype == "Int64":
        columns[col] = "BIGINT"
      elif dtype == "Float64":
        columns[col] = "DECIMAL"
      elif dtype == "bool":
        columns[col] = "BOOLEAN"
      else:
        columns[col] = "VARCHAR(4096)"
    create = "CREATE TABLE IF NOT EXISTS {} (".format(ft.name)
    for col, dtype in columns.items():
      create += "{} {},".format(col, dtype)
    create = create[:-1] + ")"
    engine.execute(create)
    return to_ingest

  def ingest_from_query(self, query: str, ft: FeatureTable, expected_columns: typing.List[str]) -> None:
    raise NotImplementedError("RedshiftIngestion.ingest_from_query is not implemented")