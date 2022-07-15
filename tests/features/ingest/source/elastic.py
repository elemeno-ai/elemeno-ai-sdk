import mock
from mock import patch
import pytest

from elemeno_ai_sdk.ml.features.ingest.source.elastic import ElasticIngestionSource

@patch('elasticsearch.Elasticsearch')
@pytest.fixture
def mocked_elastic():
  with mock.patch("elasticsearch.Elasticsearch.search") as mocked_search, \
        mock.patch("elasticsearch.client.IndicesClient.create") as mocked_index_create:
    mocked_search.return_value = "pipopapu"
    mocked_index_create.return_value = {"acknowledged": True}

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