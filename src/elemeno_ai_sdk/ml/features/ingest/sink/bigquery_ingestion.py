from datetime import datetime
import json
import typing
import feast

import pandas as pd
from google.cloud import bigquery
from elemeno_ai_sdk import logger
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable
from elemeno_ai_sdk.ml.features.types import FeatureType
from elemeno_ai_sdk.ml.features.ingest.sink.base_ingestion import Ingestion
from elemeno_ai_sdk.ml.features.utils import create_insert_into

class BigQueryIngestion(Ingestion):

  def __init__(self, fs):
    super().__init__()
    self._fs = fs

  def read_table(self, query: str) -> pd.DataFrame:
    raise NotImplementedError("This method is not implemented yet.")

  def ingest(self, to_ingest: pd.DataFrame, ft: FeatureTable, renames: typing.Optional[typing.Dict[str, str]] = None, expected_columns: typing.List[str] = [], schema: typing.List[typing.Dict] = None) -> None:
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

  def staging_ingest(self, to_ingest: pd.DataFrame, name: str) -> None:
    raise(NotImplementedError("This method is not implemented yet."))
  
  def ingest_schema(self, feature_table: FeatureTable, schema_file_path: str) -> str:
    """
    Use this method if you want to use a jsonschema file to create the feature table
    If other entities/features were registered, this method will append the ones in the jsonschema to them

    args:
    - schema_file_path: str - The local path to the file containing the jsonschema definition

    """
    with open(schema_file_path, mode="r", encoding="utf-8") as schema_file:
      jschema = json.loads(schema_file.read())
      table_schema = []
      pd_schema = {}
      for name, prop in jschema["properties"].items():
        fmt = prop["format"] if "format" in prop else None
        table_schema.append({"name": name, "type": FeatureType.from_str_to_bq_type(prop["type"], format=fmt).name})
        pd_schema[name] = pd.Series(dtype=FeatureType.from_str_to_pd_type(prop["type"], format=fmt))
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

      logger.debug("FT bq types schema: %s", table_schema)
      feature_table.set_table_schema(table_schema)
      logger.debug("Pandas types schema: %s", pd_schema)
      dataframe = pd.DataFrame(pd_schema)
      project_id = self._fs.config.offline_store.project_id
      dataset = self._fs.config.offline_store.dataset
      location = self._fs.config.offline_store.location
      dataframe.to_gbq(destination_table=f"{dataset}.{feature_table.name}",
          project_id=project_id, if_exists="append", location=location)

  def get_last_row(self, feature_table: 'FeatureTable', date_from: typing.Optional[datetime] = None, where: typing.Optional[typing.Dict[str, typing.Any]] = None) -> pd.DataFrame:
    raise(NotImplementedError("This method is not implemented yet."))

def create_table(self, to_ingest: pd.DataFrame, ft_name: str, engine: typing.Any):
  #TODO: implement
  raise NotImplementedError("BigQueryIngestion.create_table is not implemented.")

