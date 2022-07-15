
from elemeno_ai_sdk.ml.features.feature_store import FeatureStore
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable
from elemeno_ai_sdk.ml.features.ingest.sink.ingestion_sink_builder import IngestionSinkType



def test_get_training_features():
  conn_str = "postgresql://"
  fs = FeatureStore(sink_type=IngestionSinkType.REDSHIFT, connection_string=conn_str)
  ft = FeatureTable(name="bel_test", feature_store=fs)
  print("will read training features")
  print(fs.get_training_features(ft, limit=100))

if __name__ == "__main__":
  test_get_training_features()