import feast
import os
from numpy import dtype
import pandas as pd
from elemeno_ai_sdk.ml.features.feature_store import FeatureStore
from elemeno_ai_sdk.ml.features.feature_table import FeatureTableDefinition
from sqlalchemy import create_engine

def test_ingest_rs():
  fs = FeatureStore()
  ft = FeatureTableDefinition(
    "test",
    fs,
    entities=[feast.Entity("entity_id")],
    features=[feast.Feature("feature_float", dtype=feast.ValueType.FLOAT), feast.Feature("feature_int", dtype=feast.ValueType.INT32)]
  )
  df = pd.DataFrame(
      {
          "entity_id": [1, 2, 3],
          "feature_float": [1.0, 2.0, 3.0],
          "feature_int": [1, 2, 3],
      }
  )
  data_path =  os.path.abspath(f"data/{ft.name}.db")
  if os.path.exists(data_path):
    os.remove(data_path)
  fs.ingest_rs(ft.get_view(), df, 
    f"sqlite:///{data_path}?mode=rwc", ["entity_id", "feature_float", "feature_int"],
    created_timestamp_name="created_timestamp")
  """
  " assert data was properly inserted on sqlite
  """
  conn = create_engine(f"sqlite:///{data_path}?mode=rwc", isolation_level="AUTOCOMMIT")
  try:
      df = pd.read_sql(f"select * from {ft.name}", conn)
      assert df.shape == (3, 3)
      assert df.loc[0, "feature_float"] == 1.0
      assert df.loc[1, "feature_float"] == 2.0
      assert df.loc[2, "feature_float"] == 3.0
      assert df.loc[0, "feature_int"] == 1
      assert df.loc[1, "feature_int"] == 2
      assert df.loc[2, "feature_int"] == 3
  finally:
      conn.dispose()

def test_ingest():
    fs = FeatureStore()
    ft = FeatureTableDefinition(feature_store=fs, name="test_ft", features=[
        feast.Feature(name="a", dtype=feast.ValueType.FLOAT),
        feast.Feature(name="b", dtype=feast.ValueType.FLOAT),
        feast.Feature(name="c", dtype=feast.ValueType.FLOAT)],
        entities=[feast.Entity("id", value_type=feast.ValueType.STRING)])
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
    # tests run with a dummy file offline store, lets injest the config deps we need for bq
    setattr(fs.config.offline_store, "project_id", "test")
    setattr(fs.config.offline_store, "location", "test")
    fs.ingest(ft.get_view(), df)
    assert len(fs._fs.list_feature_tables()) == 1
    assert len(fs._fs.list_feature_tables()[0].features) == 3
    assert fs._fs.list_feature_tables()[0].features[0].name == "a"
    assert fs._fs.list_feature_tables()[0].features[0].dtype == feast.ValueType.FLOAT


def test_ingest_from_query():
    fs = FeatureStore()
    ft = FeatureTableDefinition(feature_store=fs, name="test_ft", features=[
        feast.Feature(name="a", dtype=feast.ValueType.FLOAT),
        feast.Feature(name="b", dtype=feast.ValueType.FLOAT),
        feast.Feature(name="c", dtype=feast.ValueType.FLOAT)],
        entities=[feast.Entity("id", value_type=feast.ValueType.STRING)])
    # tests run with a dummy file offline store, lets injest the config deps we need for bq
    setattr(fs.config.offline_store, "project_id", "test")
    setattr(fs.config.offline_store, "location", "test")
    query = "SELECT 1 as a, 2 as b, 3 as c"
    fs.ingest_from_query(ft.get_view(), query)

    assert len(fs._fs.list_feature_tables()) == 1
    assert len(fs._fs.list_feature_tables()[0].features) == 3