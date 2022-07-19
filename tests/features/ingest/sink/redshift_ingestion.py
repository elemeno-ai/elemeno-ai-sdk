from datetime import datetime
import os
import pytest

from ..fixtures import feature_table, with_test_row, feature_store

def clean_db():
  os.remove("test.db")

def test_get_sink_last_row_happy_path(feature_store, feature_table, with_test_row):
  assert feature_store.get_sink_last_ts(feature_table) == "2020-01-01T00:00:01"
  clean_db()

def test_get_sink_last_row_empty(feature_store, feature_table):
  try:
    assert feature_store.get_training_features(feature_table).empty
    assert feature_store.get_sink_last_ts(feature_table) == None
  except Exception as e:
    assert False, "Exception was thrown: " + str(e)
  finally:
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