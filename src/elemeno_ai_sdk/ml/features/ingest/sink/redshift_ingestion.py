from datetime import datetime
import json
import typing
from xmlrpc.client import Boolean
import feast
from sqlalchemy import VARCHAR, create_engine
from sqlalchemy.sql import text
import sqlalchemy
import pandas as pd

from elemeno_ai_sdk.ml.features.feature_table import FeatureTable
from elemeno_ai_sdk.ml.features.types import FeatureType
from elemeno_ai_sdk import logger
from elemeno_ai_sdk.ml.features.ingest.sink.base_ingestion import Ingestion
from elemeno_ai_sdk.cos.minio import MinioClient

MAP_TYPES = {
  ''
}
class RedshiftIngestion(Ingestion):

  def __init__(self, fs, connection_string: str):
    super().__init__()
    self._conn_str = connection_string
    self._conn = create_engine(self._conn_str, hide_parameters=True, echo=False, isolation_level="AUTOCOMMIT")
    self.rs_types = {
      "object": "SUPER",
      "string": "VARCHAR(12600)",
      "string[python]": "VARCHAR(12600)",
      "Int64": "BIGINT",
      "Int32": "BIGINT",
      "Float64": "FLOAT",
      "bool": "BOOLEAN",
      "datetime64[ns]": "TIMESTAMP"
    }
    self._schema_dict: typing.Dict = None


  def read_table(self, query: str) -> pd.DataFrame:
    """
    Querys a Redshift table and returns a DataFrame.

    args:
    
    - query: SQL query to run

    return:
    
    - DataFrame with columns in the same order as the FeatureTable
    """
    engine = create_engine(self._conn_str, hide_parameters=False, isolation_level="AUTOCOMMIT")
    with engine.connect().execution_options(autocommit=True) as conn:
      return pd.read_sql(query, con = conn)

  def staging_ingest(self, to_ingest: pd.DataFrame, name: str) -> None:
    """
    Ingests a DataFrame to a Redshift staging table.
    
    args:
    
    - to_ingest: DataFrame to ingest
    - name: Name of the staging table
    """
    conn = self._conn
    try:
      # ingest data
      max_rows_per_insert = 5000
      insert_pages = len(to_ingest) // max_rows_per_insert + 1
      for i in range(insert_pages):
        logger.info("Ingesting page %d of %d", i, insert_pages)
        if len(to_ingest) < (i+1) * max_rows_per_insert:
          chunk = to_ingest.iloc[i*max_rows_per_insert:]
        else:
          chunk = to_ingest.iloc[i * max_rows_per_insert:(i + 1) * max_rows_per_insert]
        self._to_sql(chunk, name, conn)
    except Exception as exception:
      logger.error("Failed to ingest data to Redshift: %e", exception)
      raise exception
    finally:
      conn.dispose()
    
  
  def ingest(self, to_ingest: pd.DataFrame, ft: FeatureTable, renames: typing.Optional[typing.Dict[str, str]] = None,
      expected_columns: typing.Optional[typing.List[str]] = None) -> None:
    """"
    Ingests a DataFrame to a Redshift table.
    
    args:
    
    - to_ingest: DataFrame to ingest
    - ft: FeatureTable to ingest to
    - renames: Optional dictionary of column names to rename
    - expected_columns: Optional list of columns to check for
    """
    super().ingest(to_ingest, ft, renames, expected_columns)
    if renames is not None:
      to_ingest = to_ingest.rename(columns=renames)
    if expected_columns is None or len(expected_columns) == 0:
      logger.warning("No expected columns provided. Will ingest all columns.")
      expected_columns = to_ingest.columns.to_list()
    to_ingest = to_ingest.filter(expected_columns, axis=1)
    conn = self._conn
    try:
      logger.info("Within RedshiftIngestion.ingest, about to create table {}".format(ft.name))
      # create table if not exists
      to_ingest = self.create_table(to_ingest, ft.name, conn)
      # ingest data
      max_rows_per_insert = 5000
      insert_pages = len(to_ingest) // max_rows_per_insert + 1
      for i in range(insert_pages):
        logger.info("Ingesting page %d of %d", i, insert_pages)
        if len(to_ingest) < (i+1) * max_rows_per_insert:
          chunk = to_ingest.iloc[i*max_rows_per_insert:]
        else:
          chunk = to_ingest.iloc[i * max_rows_per_insert:(i + 1) * max_rows_per_insert]
        self._to_sql(chunk, ft.name, conn)
    except Exception as exception:
      logger.error("Failed to ingest data to Redshift: %e", exception)
      raise exception
    finally:
      conn.dispose()

  def _to_sql(self, df: pd.DataFrame, ft_name, conn) -> None:
    dtypes = {}
    if self._schema_dict is None:
      logger.info("Schema was not provided. Will attempt to infer schema from table.")
    else:
      for k, v in self._schema_dict['properties'].items():
        if "type" in v and v["type"] == "object" or v["type"] == "binary_download":
          dtypes[k] = sqlalchemy.types.JSON
    return df.to_sql(ft_name, conn, index=False, if_exists='append', method='multi', chunksize=1000, dtype=dtypes)
  
  def create_table(self, to_ingest: pd.DataFrame, ft_name: str, engine: sqlalchemy.engine.Engine) -> pd.DataFrame:
    """
    Creates a table in Redshift if it does not exist.

    args:
    
    - to_ingest: DataFrame to ingest
    - ft: FeatureTable to ingest to
    - engine: SQLAlchemy engine

    return:
    
    - DataFrame with columns in the same order as the FeatureTable
    """
    to_ingest = to_ingest.convert_dtypes()
    # FIXME: this is a hack to get around the fact we're not using FeatureTable here
    date_cols = ["created_timestamp", "create_timestamp", "event_timestamp", "created_date", "record_date", "updated_date"]
    
    columns = {}
    for col, dtype in to_ingest.dtypes.items():
      if col == 'media' or 'media' in col.split('_'):
          columns[f'{col}_status'] = 'BOOLEAN'
          to_ingest[f'{col}_status'] = False
      if col in date_cols:
        columns[col] = "TEXT"
      else:
        columns[col] = self.rs_types[dtype.name]

    if not sqlalchemy.inspect(engine).has_table(ft_name):
      create = "CREATE TABLE {} (".format(ft_name)
      for col, dtype in columns.items():
        create += "{} {},".format(col, dtype)
      create = create[:-1] + ")"
      engine.execute(create)
    return to_ingest
  
  def ingest_schema(self, feature_table: FeatureTable, schema_file_path: str) -> str:
    """
    This method should be called if you want to use a jsonschema file to create the feature table
    If other entities/features were registered, this method will append the ones in the jsonschema to them

    args:
    
    - schema_file_path: str - The local path to the file containing the jsonschema definition

    """
    try:
      with open(schema_file_path, mode="r", encoding="utf-8") as schema_file:
        jschema = json.loads(schema_file.read())
        self._schema_dict = jschema
        feature_table.schema_str = json.dumps(jschema)
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
          dummy_row[name] = FeatureType.get_dummy_value(this_pd_type) # we don't need to create this dict 
          if "isKey" in prop and prop["isKey"] == "true":
            feature_table.register_entity(feast.Entity(name=name, join_keys=[name], description=name, value_type=FeatureType.from_str_to_entity_type(prop["type"])))
          else:
            if "format" in prop and prop["format"] == "date-time":
              continue
            feature_table.register_features(feast.Field(name=name, dtype=FeatureType.from_str_to_feature_type(prop["type"])))

        if len(list(filter(lambda x: x["name"] == feature_table.created_col, table_schema))) == 0:
          table_schema.append({"name": feature_table.created_col, "type": FeatureType.from_str_to_bq_type("string", format="date-time").name})
          pd_schema[feature_table.created_col] = pd.Series(dtype=FeatureType.from_str_to_pd_type("string", format="date-time"))
        if len(list(filter(lambda x: x["name"] == feature_table.evt_col, table_schema))) == 0:
          table_schema.append({"name": feature_table.evt_col, "type": FeatureType.from_str_to_bq_type("string", format="date-time").name})
          pd_schema[feature_table.evt_col] = pd.Series(dtype=FeatureType.from_str_to_pd_type("string", format="date-time"))

        logger.info("FT types schema: %s", table_schema)
        feature_table.set_table_schema(table_schema)
        dummy_df = pd.DataFrame(pd_schema)
        if len(adjusted_dtypes) == 0:
          adjusted_dtypes = None
        dummy_df = dummy_df.append(dummy_row, ignore_index=True) # we don't need to append the dummy row here
        #TODO Bruno - When there's any column with type binary_download in the schema, create an auxiliary feature_table with the list of files to download for each entity
        conn = create_engine(self._conn_str, hide_parameters=True, echo=False, isolation_level="AUTOCOMMIT")
        self.create_table(dummy_df, feature_table.name, conn)
        return json.dumps(jschema)
    except Exception as exception:
      raise exception

  def get_last_row(self, feature_table: 'FeatureTable', date_from: typing.Optional[datetime] = None, where: typing.Optional[typing.Dict[str, typing.Any]] = None) -> pd.DataFrame:
    conn = create_engine(self._conn_str, hide_parameters=False, isolation_level="AUTOCOMMIT")
    _where = ""
    if date_from != None:
      _where = "WHERE {} > '{}'".format(feature_table.evt_col, date_from.strftime("%Y-%m-%d %H:%M:%S"))
    if where != None:
      if _where != "":
        _where += " AND "
      else:
        _where = "WHERE "
      for k,v in where.items():
        _where += "{} = '{}' AND ".format(k, v)
      _where = _where[:-4]
    _order_by = "ORDER BY {} DESC LIMIT 1".format(feature_table.created_col)
    return pd.read_sql(
      f"SELECT {feature_table.evt_col} FROM {feature_table.name} {_where} {_order_by}", 
      conn)

  def ingest_from_query(self, query: str, ft: FeatureTable) -> None:
    raise NotImplementedError("RedshiftIngestion.ingest_from_query is not implemented")
