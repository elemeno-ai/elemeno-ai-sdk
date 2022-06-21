from datetime import datetime
import json
import typing
import feast
from sqlalchemy import VARCHAR, create_engine
from sqlalchemy.sql import text
import sqlalchemy
import pandas as pd

from elemeno_ai_sdk.ml.features.feature_table import FeatureTable
from elemeno_ai_sdk.ml.features.types import FeatureType
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
  
  def ingest_schema(self, feature_table: FeatureTable, schema_file_path: str) -> None:
    """
    This method should be called if you want to use a jsonschema file to create the feature table
    If other entities/features were registered, this method will append the ones in the jsonschema to them

    Arguments:
    schema_file_path: str - The local path to the file containing the jsonschema definition

    """
    try:
      with open(schema_file_path, mode="r", encoding="utf-8") as schema_file:
        jschema = json.loads(schema_file.read())
        table_schema = []
        pd_schema = {}
        adjusted_dtypes = {}
        dummy_row = {}
        for name, prop in jschema["properties"].items():
          
          fmt = prop["format"] if "format" in prop else None
          if prop["type"] == "string" and "size" in prop:
            adjusted_dtypes[name] = VARCHAR(int(prop["size"]))
          table_schema.append({"name": name, "type": FeatureType.from_str_to_bq_type(prop["type"], format=fmt).name})
          this_pd_type = FeatureType.from_str_to_pd_type(prop["type"], format=fmt)
          pd_schema[name] = pd.Series(dtype=this_pd_type)
          dummy_row[name] = FeatureType.get_dummy_value(this_pd_type)
          if "isKey" in prop and prop["isKey"] == "true":
            feature_table.register_entity(feast.Entity(name=name, description=name, value_type=FeatureType.from_str_to_feature_type(prop["type"])))
          else:
            if "format" in prop and prop["format"] == "date-time":
              continue
            feature_table.register_features(feast.Feature(name, FeatureType.from_str_to_feature_type(prop["type"])))

        if len(list(filter(lambda x: x["name"] == feature_table.created_col, table_schema))) == 0:
          table_schema.append({"name": feature_table.created_col, "type": FeatureType.from_str_to_bq_type("string", format="date-time").name})
          pd_schema[feature_table.created_col] = pd.Series(dtype=FeatureType.from_str_to_pd_type("string", format="date-time"))
        if len(list(filter(lambda x: x["name"] == feature_table.evt_col, table_schema))) == 0:
          table_schema.append({"name": feature_table.evt_col, "type": FeatureType.from_str_to_bq_type("string", format="date-time").name})
          pd_schema[feature_table.evt_col] = pd.Series(dtype=FeatureType.from_str_to_pd_type("string", format="date-time"))

        logger.info("FT types schema: %s", table_schema)
        feature_table.set_table_schema(table_schema)
        conn = create_engine(self._conn_str, hide_parameters=True, isolation_level="AUTOCOMMIT")
        dummy_df = pd.DataFrame(pd_schema)
        if len(adjusted_dtypes) == 0:
          adjusted_dtypes = None
        dummy_df.append(dummy_row)
        dummy_df.to_sql(f"{feature_table.name}",
                conn, index=False, if_exists='append', dtype=adjusted_dtypes)
    except Exception as exception:
      raise exception

  def ingest_from_query(self, query: str, ft: FeatureTable) -> None:
    raise NotImplementedError("RedshiftIngestion.ingest_from_query is not implemented")