import unittest
from unittest.mock import patch

from elemeno_ai_sdk.ml.features.feature_store import FeatureStore
from elemeno_ai_sdk.ml.features.ingest.sink.ingestion_sink_builder import FileIngestionSinkType, IngestionSinkType
from elemeno_ai_sdk.ml.features.ingest.source.ingestion_source_builder import IngestionSourceType


class TestFeatureStore(unittest.TestCase):
    @patch("feast.FeatureStore")
    @patch.dict("os.environ", {"ELEMENO_CFG_FILE": "tests/elemeno.yaml"})
    def test_init_valid_parameters(self, mock_feast_feature_store):
        feature_store = FeatureStore(
            sink_type=IngestionSinkType.BIGQUERY,
            source_type=IngestionSourceType.BIGQUERY,
            config={
                "feature_store": {
                    "source": {
                        "params": {
                            "project_id": "test_project_id",
                        }
                    }
                }
            },
        )
        self.assertIsNotNone(feature_store)
        self.assertEqual(feature_store._sink_type, IngestionSinkType.BIGQUERY)
        self.assertEqual(feature_store._source_type, IngestionSourceType.BIGQUERY)
        mock_feast_feature_store.assert_called_once()

    @patch("feast.FeatureStore")
    @patch.dict("os.environ", {"ELEMENO_CFG_FILE": "tests/elemeno.yaml"})
    def test_init_valid_parameters_redshift(self, mock_feast_feature_store):
        feature_store = FeatureStore(
            sink_type=IngestionSinkType.REDSHIFT,
            source_type=IngestionSourceType.REDSHIFT,
            config={
                "feature_store": {
                    "sink": {
                        "type": "REDSHIFT",
                        "params": {
                            "host": "example.com",
                            "port": 5439,
                            "user": "username",
                            "password": "password",
                            "dbname": "dbname",
                        },
                    },
                    "source": {
                        "type": "REDSHIFT",
                        "params": {
                            "host": "example.com",
                            "port": 5439,
                            "cluster_name": "cluster_name",
                            "user": "username",
                            "password": "password",
                            "database": "dbname",
                        },
                    },
                }
            },
        )
        self.assertIsNotNone(feature_store)
        self.assertEqual(feature_store._sink_type, IngestionSinkType.REDSHIFT)
        self.assertEqual(feature_store._source_type, IngestionSourceType.REDSHIFT)
        mock_feast_feature_store.assert_called_once()

    @patch("feast.FeatureStore")
    @patch.dict("os.environ", {"ELEMENO_CFG_FILE": "tests/elemeno.yaml"})
    def test_init_valid_parameters_elastic_bigquery(self, mock_feast_feature_store):
        feature_store = FeatureStore(
            sink_type=IngestionSinkType.BIGQUERY,
            source_type=IngestionSourceType.ELASTIC,
            config={"feature_store": {"sink": {"type": "BIGQUERY"}}},
        )
        self.assertIsNotNone(feature_store)
        self.assertEqual(feature_store._sink_type, IngestionSinkType.BIGQUERY)
        self.assertEqual(feature_store._source_type, IngestionSourceType.ELASTIC)
        mock_feast_feature_store.assert_called_once()

    @patch("feast.FeatureStore")
    @patch.dict("os.environ", {"ELEMENO_CFG_FILE": "tests/elemeno.yaml"})
    def test_init_valid_parameters_gcs_bigquery(self, mock_feast_feature_store):
        feature_store = FeatureStore(
            sink_type=IngestionSinkType.BIGQUERY,
            source_type=IngestionSourceType.GCS,
            config={
                "feature_store": {
                    "sink": {"type": "BIGQUERY"},
                    "source": {"type": "GCS", "params": {"bucket": "test_bucket", "path": "test_path"}},
                }
            },
        )
        self.assertIsNotNone(feature_store)
        self.assertEqual(feature_store._sink_type, IngestionSinkType.BIGQUERY)
        self.assertEqual(feature_store._source_type, IngestionSourceType.GCS)
        mock_feast_feature_store.assert_called_once()

    @patch("feast.FeatureStore")
    @patch.dict("os.environ", {"ELEMENO_CFG_FILE": "tests/elemeno.yaml"})
    def test_init_valid_parameters_minio_file_sink(self, mock_feast_feature_store):
        feature_store = FeatureStore(
            file_sink_type=FileIngestionSinkType.MINIO,
            config={"feature_store": {"feast_config_path": "some/path", "sink": {"type": "BIGQUERY"}}},
        )
        self.assertIsNotNone(feature_store)
        self.assertEqual(feature_store._file_sink_type, FileIngestionSinkType.MINIO)
        mock_feast_feature_store.assert_called_once()

    @patch("feast.FeatureStore")
    @patch.dict("os.environ", {"ELEMENO_CFG_FILE": "tests/elemeno.yaml"})
    def test_init_missing_sink_and_source_types(self, mock_feast_feature_store):
        feature_store = FeatureStore()
        self.assertIsNotNone(feature_store)
        self.assertIsNone(feature_store._sink_type)
        self.assertIsNone(feature_store._source_type)
        mock_feast_feature_store.assert_called_once()

    @patch("feast.FeatureStore")
    @patch.dict("os.environ", {"ELEMENO_CFG_FILE": "tests/elemeno.yaml"})
    def test_init_with_invalid_sink_type(self, mock_feast_feature_store):
        with self.assertRaises(Exception) as context:
            FeatureStore(sink_type="INVALID_SINK", config={"feature_store": {"sink": {"type": "BIGQUERY"}}})
        self.assertIn("Unsupported sink type", str(context.exception))

    @patch("feast.FeatureStore")
    @patch.dict("os.environ", {"ELEMENO_CFG_FILE": "tests/elemeno.yaml"})
    def test_init_with_invalid_source_type(self, mock_feast_feature_store):
        with self.assertRaises(Exception) as context:
            FeatureStore(source_type="INVALID_SOURCE", config={"feature_store": {"sink": {"type": "BIGQUERY"}}})
        self.assertIn("Unsupported source type", str(context.exception))


if __name__ == "__main__":
    unittest.main()
