from elemeno_ai_sdk.ml.features.feature_store import FeatureStore
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable
from elemeno_ai_sdk.ml.features.ingest.sink.ingestion_sink_builder import IngestionSinkBuilder, IngestionSinkType
from elemeno_ai_sdk.ml.features.ingest.source.elastic import ElasticIngestionSource


def test_elastic_ingestion():
    conn_str = (
        "postgresql://USER:PASS@redshift-cluster-2.cwuptesab6o8.us-east-1.redshift.amazonaws.com:5439/lucas-elemeno-ai"
    )
    fs = FeatureStore(sink_type=IngestionSinkType.REDSHIFT, connection_string=conn_str)
    ft = FeatureTable(name="test_5", feature_store=fs)

    es = ElasticIngestionSource(host="HOST", username="elastic", password="PASS")
    query = {"query_string": {"query": "epoch_second(created_date)>0"}}
    to_ingest = es.read("listing-br-rj-rio.de.janeiro-copacabana", query)

    fs.ingest(ft, to_ingest)
    print("end")


if __name__ == "__main__":
    test_elastic_ingestion()
