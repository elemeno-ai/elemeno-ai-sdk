import os
from datetime import datetime
from unittest.mock import Mock

import mock
import pandas as pd
import pytest
from feast import Entity, Feature, ValueType
from mock import patch

from elemeno_ai_sdk.config import Configs
from elemeno_ai_sdk.ml.features.feature_store import FeatureStore
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable
from elemeno_ai_sdk.ml.features.ingest.sink.ingestion_sink_builder import IngestionSinkType
from elemeno_ai_sdk.ml.features.ingest.sink.redshift_ingestion import RedshiftIngestion
from elemeno_ai_sdk.ml.features.ingest.source.elastic import ElasticIngestionSource
from elemeno_ai_sdk.ml.features.ingest.source.ingestion_source_builder import (
    IngestionSourceBuilder,
    IngestionSourceType,
)

from ..fixtures import feature_store, feature_table


@patch("elasticsearch.Elasticsearch")
@pytest.fixture
def mocked_elastic():
    with mock.patch("elasticsearch.Elasticsearch.search") as mocked_search, mock.patch(
        "elasticsearch.client.IndicesClient.create"
    ) as mocked_index_create:
        mocked_search.return_value = "pipopapu"
        mocked_index_create.return_value = {"acknowledged": True}


@pytest.fixture
def build_elastic_mock():
    os.environ["ELEMENO_CFG_FILE"] = "./elemeno.yaml.TEMPLATE"
    return mock.patch(
        "elemeno_ai_sdk.ml.features.ingest.source.ingestion_source_builder.IngestionSourceBuilder.build_elastic"
    )


@pytest.fixture
def fs_fixture():
    os.environ["ELEMENO_CFG_FILE"] = "./elemeno.yaml.TEMPLATE"
    with mock.patch(
        "elemeno_ai_sdk.ml.features.ingest.source.ingestion_source_builder.IngestionSourceBuilder.build_elastic"
    ) as build_elastic, mock.patch(
        "elemeno_ai_sdk.ml.features.ingest.source.elastic.ElasticIngestionSource"
    ) as elastic_mock:
        build_elastic.return_value = elastic_mock
        f = FeatureStore(source_type=IngestionSourceType.ELASTIC, sink_type=IngestionSinkType._REDSHIFT_UNIT_TESTS)
        return (f, elastic_mock)


@pytest.fixture
def test_mem():
    mem = {}
    mem["json_match_schema"] = {
        "hits": {
            "hits": [
                {"_source": {"listingId": "123a", "aggregationIds": ["abc"]}},
                {"_source": {"listingId": "zz2", "aggregationIds": ["abc"]}},
            ]
        }
    }

    mem["json_add_schema"] = {
        "hits": {
            "hits": [
                {"_source": {"listingId": "123a", "aggregationIds": ["abc"]}, "other_column": "ax"},
                {"_source": {"listingId": "zz2", "aggregationIds": ["abc"], "other_column": "ax"}},
            ]
        }
    }
    return mem


def test_read_happy_path(mocked_elastic):
    source = ElasticIngestionSource("http://localhost:9200", "username", "password")
    with mock.patch("elasticsearch.Elasticsearch.count") as mocked_count, mock.patch(
        "elasticsearch.Elasticsearch.search"
    ) as mocked_search:
        mocked_count.return_value = {"count": 10}
        mocked_search.return_value = {"hits": {"hits": [{"_source": {"id": "1"}}, {"_source": {"id": "2"}}]}}
        result = source.read("index", "query", 10)
        print(result)
        assert len(result) == 2
        assert result.iloc[0]["id"] == "1"
        assert result.iloc[1]["id"] == "2"


def test_read_from_elastic(mocked_elastic, fs_fixture, feature_table):
    feature_store = fs_fixture[0]
    elastic_mock = fs_fixture[1]
    query = {"query": {"match_all": {}}}
    with mock.patch("elasticsearch.Elasticsearch") as elastic_module:
        r = feature_store.read_and_ingest_from_query(feature_table, query, max_per_page=10, index="test-index")
        elastic_mock.read.assert_called_once_with(query=query, index="test-index", max_per_page=10)
        pass


def test_read_from_elastic_check_calls(feature_table):
    os.environ["ELEMENO_CFG_FILE"] = "tests/elemeno.yaml"
    Configs.instance(force_reload=True)
    with mock.patch("elemeno_ai_sdk.ml.features.ingest.source.elastic.Elasticsearch") as elastic_module:
        # build_elastic_mock.return_value = elastic_module
        elastic_module.count.return_value = {"count": 10}
        elastic_module.search.return_value = {"hits": {"hits": [{"_source": {"id": "1"}}, {"_source": {"id": "2"}}]}}
        elastic_module.configure_mock()
        f = FeatureStore(source_type=IngestionSourceType.ELASTIC, sink_type=IngestionSinkType._REDSHIFT_UNIT_TESTS)
        f._source._es = elastic_module
        f._sink = Mock()
        query = {"query": {"match_all": {}}}
        f.read_and_ingest_from_query(feature_table, query, max_per_page=10, index="test-index")
        elastic_module.count.assert_called_once_with(index="test-index", query={"query": {"match_all": {}}})
        elastic_module.search.assert_called_once_with(index="test-index", query={"query": {"match_all": {}}}, size=10)
        pass


def test_read_after_elastic_check_calls(feature_table):
    os.environ["ELEMENO_CFG_FILE"] = "tests/elemeno.yaml"
    Configs.instance(force_reload=True)
    with mock.patch("elemeno_ai_sdk.ml.features.ingest.source.elastic.Elasticsearch") as elastic_module:
        # build_elastic_mock.return_value = elastic_module
        elastic_module.count.return_value = {"count": 10}
        elastic_module.search.return_value = {"hits": {"hits": [{"_source": {"id": "1"}}, {"_source": {"id": "2"}}]}}
        elastic_module.configure_mock()
        f = FeatureStore(source_type=IngestionSourceType.ELASTIC, sink_type=IngestionSinkType._REDSHIFT_UNIT_TESTS)
        f._source._es = elastic_module
        f._sink = Mock()
        query = {"query": {"match_all": {}}}
        f.read_and_ingest_from_query_after(
            feature_table, query, max_per_page=10, index="test-index", after="2022-01-01T00:00:00"
        )
        elastic_module.count.assert_called_once_with(
            index="test-index",
            query={"query": {"match_all": {}, "range": {"event_timestamp": {"gt": "2022-01-01T00:00:00"}}}},
        )
        elastic_module.search.assert_called_once_with(
            index="test-index",
            query={"query": {"match_all": {}, "range": {"event_timestamp": {"gt": "2022-01-01T00:00:00"}}}},
            size=10,
        )
        pass


def test_read_after_with_more_columns_and_ingest(feature_table, test_mem):
    os.environ["ELEMENO_CFG_FILE"] = "tests/elemeno.yaml"
    Configs.instance(force_reload=True)
    with mock.patch("elemeno_ai_sdk.ml.features.ingest.source.elastic.Elasticsearch") as elastic_module:
        elastic_module.count.return_value = {"count": 10}
        elastic_module.search.return_value = test_mem["json_add_schema"]
        elastic_module.configure_mock()
        f = FeatureStore(source_type=IngestionSourceType.ELASTIC, sink_type=IngestionSinkType._REDSHIFT_UNIT_TESTS)
        f._source._es = elastic_module
        f._sink = Mock()
        query = {"query": {"match_all": {}}}
        f.read_and_ingest_from_query_after(
            feature_table, query, max_per_page=10, index="test-index", after="2022-01-01T00:00:00"
        )
        assert feature_table.features == [Feature("aggregationIds", ValueType.BYTES)]
        assert feature_table.entities == [
            Entity("listingId", ValueType.STRING, description="listingId", join_key="listingId")
        ]
        f._sink.ingest.assert_called_once()
        call_args = f._sink.ingest.call_args
        df = call_args[0][0]
        feature_columns = df.columns.tolist()
        assert feature_columns == ["listingId", "aggregationIds", "other_column"]
        print(call_args.kwargs)
        all_columns_param = call_args[0][3]
        assert all_columns_param == ["listingId", "aggregationIds"]
        pass


def test_read_with_more_columns_and_ingest(feature_table, test_mem):
    os.environ["ELEMENO_CFG_FILE"] = "tests/elemeno.yaml"
    Configs.instance(force_reload=True)
    with mock.patch("elemeno_ai_sdk.ml.features.ingest.source.elastic.Elasticsearch") as elastic_module:
        elastic_module.count.return_value = {"count": 10}
        elastic_module.search.return_value = test_mem["json_add_schema"]
        elastic_module.configure_mock()
        f = FeatureStore(source_type=IngestionSourceType.ELASTIC, sink_type=IngestionSinkType._REDSHIFT_UNIT_TESTS)
        f._source._es = elastic_module
        f._sink = Mock()
        query = {"query": {"match_all": {}}}
        f.read_and_ingest_from_query(feature_table, query, max_per_page=10, index="test-index")
        f._sink.ingest.assert_called_once()
        call_args = f._sink.ingest.call_args
        df = call_args[0][0]
        feature_columns = df.columns.tolist()
        assert feature_columns == ["listingId", "aggregationIds", "other_column"]
        all_columns_param = call_args[0][3]
        assert all_columns_param == ["listingId", "aggregationIds", "created_timestamp", "event_timestamp"]
        pass


def test_read_from_elastic_timestamps_are_added(feature_table):
    os.environ["ELEMENO_CFG_FILE"] = "tests/elemeno.yaml"
    Configs.instance(force_reload=True)
    with mock.patch("elemeno_ai_sdk.ml.features.ingest.source.elastic.Elasticsearch") as elastic_module:
        elastic_module.count.return_value = {"count": 100}
        elastic_module.search.return_value = {
            "hits": {
                "hits": [
                    {"_source": {"listingId": "1", "aggregationIds": "123"}},
                    {"_source": {"listingId": "2", "aggregationIds": "444"}},
                ]
            }
        }
        elastic_module.configure_mock()
        sink = RedshiftIngestion(None, "sqlite:///test.db?mode=rwc")
        f = FeatureStore(source_type=IngestionSourceType.ELASTIC, sink_type=IngestionSinkType.REDSHIFT)
        f._sink = sink
        f._source._es = elastic_module
        with mock.patch.object(f._sink, "_to_sql") as mock_sink:
            # mock.patch.object(IngestionSourceBuilder, "build_elastic") as build_elastic_mock:
            query = {"query": {"match_all": {}}}
            expected_df = pd.DataFrame(
                [
                    {
                        "listingId": "1",
                        "aggregationIds": "123",
                        "created_timestamp": datetime(2022, 1, 2).isoformat(),
                        "event_timestamp": datetime(2022, 1, 2).isoformat(),
                    },
                    {
                        "listingId": "2",
                        "aggregationIds": "444",
                        "created_timestamp": datetime(2022, 1, 2).isoformat(),
                        "event_timestamp": datetime(2022, 1, 2).isoformat(),
                    },
                ]
            )
            f.read_and_ingest_from_query(feature_table, query, max_per_page=100, index="test-index")
            elastic_module.count.assert_called_once_with(index="test-index", query={"query": {"match_all": {}}})
            elastic_module.search.assert_called_with(index="test-index", query={"query": {"match_all": {}}}, size=100)
            df = mock_sink.call_args[0][0]
            assert expected_df.columns.tolist() == df.columns.tolist()
            assert len(expected_df) == len(df)
            pass
