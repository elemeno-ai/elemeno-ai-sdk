import os
import mock
from mock import patch
import pytest
from elemeno_ai_sdk.ml.features.feature_store import FeatureStore

from elemeno_ai_sdk.ml.features.ingest.source.elastic import ElasticIngestionSource
from elemeno_ai_sdk.ml.features.ingest.source.ingestion_source_builder import IngestionSourceType

@patch('elasticsearch.Elasticsearch')
@pytest.fixture
def mocked_elastic():
  with mock.patch("elasticsearch.Elasticsearch.search") as mocked_search, \
        mock.patch("elasticsearch.client.IndicesClient.create") as mocked_index_create:
    mocked_search.return_value = "pipopapu"
    mocked_index_create.return_value = {"acknowledged": True}

@pytest.fixture
def feature_store():
  os.environ["ELEMENO_CFG_FILE"] = "./elemeno.yaml.TEMPLATE"
  with mock.patch("elemeno_ai_sdk.ml.features.ingest.source.ingestion_source_builder.IngestionSourceBuilder.build_elastic"):
    f = FeatureStore(sink_type=None, source_type=IngestionSourceType.ELASTIC)
    return f

def test_read_happy_path(mocked_elastic):
  source = ElasticIngestionSource("http://localhost:9200", "username", "password")
  with mock.patch("elasticsearch.Elasticsearch.count") as mocked_count, \
    mock.patch("elasticsearch.Elasticsearch.search") as mocked_search:
    mocked_count.return_value = {"count": 10}
    mocked_search.return_value = {"hits": {"hits": [{"_source": {"id": "1"}}, {"_source": {"id": "2"}}]}}
    result = source.read("index", "query", 10)
    print(result)
    assert len(result) == 2
    assert result.iloc[0]["id"] == "1"
    assert result.iloc[1]["id"] == "2"

def test_read_from_feature_store(mocked_elastic, feature_store):
  query = {"query": {"match_all": {}}}
  r = feature_store.read_and_ingest_from_query(query, 10, index="test-index")
  print(r)
  pass