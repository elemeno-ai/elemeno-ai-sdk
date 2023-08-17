import os
from datetime import datetime
from unittest import mock

import numpy as np
import pandas as pd
import sqlalchemy

from elemeno_ai_sdk.ml.features.ingest.sink.redshift_ingestion import RedshiftIngestion


def clean_db():
    os.remove("test.db")


def test_get_sink_last_row_happy_path(feature_store, feature_table, with_test_row):
    assert feature_store.get_sink_last_ts(feature_table) == "2020-01-01T00:00:01"
    clean_db()


def test_get_sink_last_row_empty(feature_store, feature_table):
    try:
        assert feature_store.get_training_features(feature_table).empty
        assert feature_store.get_sink_last_ts(feature_table) is None
    except Exception as e:
        assert False, "Exception was thrown: " + str(e)
    finally:
        clean_db()


def test_get_training_features_happy_path(feature_table, feature_store, with_test_row):
    df = feature_store.get_training_features(feature_table)
    assert len(df) == 2
    assert df.columns.tolist() == ["listingId", "aggregationIds", "created_timestamp", "event_timestamp"]


def test_get_training_features_from_ts(feature_table, feature_store, with_test_row):
    df = feature_store.get_training_features(feature_table, date_from=datetime(2020, 1, 1))
    assert len(df) == 1
    assert df.columns.tolist() == ["listingId", "aggregationIds", "created_timestamp", "event_timestamp"]
    assert df.iloc[0]["created_timestamp"] == "2020-01-01T00:00:01"


def test_create_table_dtypes(feature_store, feature_table):
    with mock.patch("elemeno_ai_sdk.ml.features.ingest.sink.redshift_ingestion.sqlalchemy") as sqlalchemy_mock:
        source = RedshiftIngestion(feature_store, "sqlite:///test.db?mode=rwc")
        to_ingest = pd.DataFrame([{"id": 1, "aggregation_ids": ["12", "abc"]}])
        engine_mock = mock.Mock()
        sqlalchemy_mock.create_engine.return_value = engine_mock
        has_table_check = mock.Mock()
        sqlalchemy_mock.inspect.return_value = has_table_check
        has_table_check.has_table.return_value = False
        source.create_table(to_ingest, feature_table, engine_mock)
        engine_mock.execute.assert_called_once_with("CREATE TABLE test_table (id BIGINT,aggregation_ids SUPER)")


@mock.patch.object(sqlalchemy, "create_engine")
@mock.patch.object(sqlalchemy, "inspect")
def test_ingest_df(mock_sqlalchemy, feature_table):
    emock = mock.Mock()
    mock_sqlalchemy.return_value = emock
    sink = RedshiftIngestion(None, "sqlite:///test.db?mode=rwc")
    sink._conn = emock
    to_ingest = pd.DataFrame([{"id": 1, "aggregation_ids": ["12", "abc"]}])
    with mock.patch("pandas.DataFrame.to_sql") as pd_mock:
        sink.ingest(to_ingest, feature_table, expected_columns=["id", "aggregation_ids"])
        pd_mock.assert_called_once_with(
            feature_table.name, emock, chunksize=1000, if_exists="append", index=False, method="multi"
        )


@mock.patch.object(sqlalchemy, "create_engine")
@mock.patch.object(sqlalchemy, "inspect")
@mock.patch.object(pd.io.sql, "to_sql")
def test_ingest_df_with_str_list(mock_sqlalchemy, feature_table, mock_pandas):
    emock = mock.Mock()
    mock_sqlalchemy.return_value = emock
    sink = RedshiftIngestion(None, "sqlite:///test.db?mode=rwc")
    sink._conn = emock
    to_ingest = pd.DataFrame([{"id": 1, "aggregation_ids": ["12", "abc"]}])
    with mock.patch.object(sink, "_to_sql") as to_sql_mock:
        sink.ingest(to_ingest, feature_table, expected_columns=["id", "aggregation_ids"])
        to_sql_mock.assert_called_once()
        internal_df_types = to_sql_mock.call_args[0][0].dtypes.tolist()
        internal_df_types = [x.type for x in internal_df_types]
        assert internal_df_types == [np.dtype("int64"), np.dtype("str")]


@mock.patch.object(sqlalchemy, "create_engine")
def test_ingest_schema(mock_create_engine):
    ftmock = mock.Mock()
    ftmock.created_col = "created_timestamp"
    ftmock.evt_col = "event_timestamp"
    ftmock.dtypes.items.return_value = [("id", "int64"), ("aggregation_ids", "str")]
    sink = RedshiftIngestion(None, "sqlite:///test.db?mode=rwc")
    sink.create_table = mock.Mock()
    sink.ingest_schema(ftmock, "tests/schema/test_schema.json")
    ftmock.set_table_schema.assert_called_once_with(
        [
            {"name": "listingId", "type": "STRING"},
            {"name": "aggregationIds", "type": "ARRAY"},
            {"name": "created_timestamp", "type": "TIMESTAMP"},
            {"name": "event_timestamp", "type": "TIMESTAMP"},
        ]
    )
