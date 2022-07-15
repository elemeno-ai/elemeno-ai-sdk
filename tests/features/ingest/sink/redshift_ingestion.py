from datetime import datetime
import os
import pandas as pd
import pytest

from elemeno_ai_sdk.ml.features.feature_store import FeatureStore
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable


@pytest.fixture
def feature_store():
  os.environ["ELEMENO_CFG_FILE"] = "./elemeno.yaml.TEMPLATE"
  f = FeatureStore(sink_type="RedshiftUnitTests", source_type=None)
  return f

@pytest.fixture
def feature_table(feature_store):
  ft = FeatureTable(feature_store=feature_store, name="test_table")
  feature_store.ingest_schema(ft, "tests/schema/test_schema.json")
  return ft

@pytest.fixture
def with_test_row(feature_table, feature_store):
  feature_store.ingest(feature_table, pd.DataFrame([
    {"listingId": 1, "aggregationIds": 123, "created_timestamp": "2020-01-01T00:00:01", "event_timestamp": "2020-01-01T00:00:01"},
    {"listingId": 2, "aggregationIds": 123, "created_timestamp": "2019-12-01T00:00:01", "event_timestamp": "2019-11-28T00:00:01"}])
  )

def clean_db():
  os.remove("test.db")

def test_get_sink_last_row_happy_path(feature_store, feature_table, with_test_row):
  assert feature_store.get_sink_last_ts(feature_table) == "2020-01-01T00:00:01"
  clean_db()

def test_get_sink_last_row_empty(feature_store, feature_table):
  assert feature_store.get_training_features(feature_table).empty
  assert feature_store.get_sink_last_ts(feature_table) == None
  clean_db()

def test_get_training_features_happy_path(feature_table, feature_store, with_test_row):
  df = feature_store.get_training_features(feature_table)
  assert len(df) == 2
  assert df.columns.tolist() == ["listingId", "aggregationIds", "created_timestamp", "event_timestamp"]

def test_get_training_features_from_ts(feature_table, feature_store, with_test_row):
  df = feature_store.get_training_features(feature_table, date_from=datetime(2020,1,1))
  assert len(df) == 1
  assert df.columns.tolist() == ["listingId", "aggregationIds", "created_timestamp", "event_timestamp"]
  assert df.iloc[0]['created_timestamp'] == '2020-01-01T00:00:01'