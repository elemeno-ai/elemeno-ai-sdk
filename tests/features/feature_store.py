import feast
import os
import pandas as pd
from elemeno_ai_sdk.features.feature_store import FeatureStore
from elemeno_ai_sdk.features.feature_table import FeatureTableDefinition
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
  fs.ingest_rs(ft.get_view(), df, f"sqlite:///{data_path}?mode=rwc", ["entity_id", "feature_float", "feature_int"])
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