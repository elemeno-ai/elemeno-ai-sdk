import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import feast
import pandas as pd

from elemeno_ai_sdk.ml.features.feature_store import FeatureStore
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable
from elemeno_ai_sdk.ml.features.ingest.source.base_source import ReadResponse


@patch.dict("os.environ", {"ELEMENO_CFG_FILE": "tests/elemeno.yaml"})
class TestFeatureStoreIngestResponse(unittest.TestCase):
    @patch.object(FeatureStore, "_has_medias", return_value=False)
    @patch.object(FeatureStore, "ingest")
    def test_ingest_response_no_rename_no_filter(self, mock_ingest, mock_has_medias):
        # Setup
        feast.FeatureStore = MagicMock()
        feature_store = FeatureStore()
        feature_table = FeatureTable(
            name="test_table",
            feature_store=feast.FeatureStore(),
            features=[feast.Feature(name="feature1", dtype=feast.ValueType.INT64)],
        )

        data = pd.DataFrame(
            {
                "feature1": [1, 2, 3],
                "event_timestamp": [datetime.utcnow()] * 3,
                "created_timestamp": [datetime.utcnow()] * 3,
            }
        )

        read_response = ReadResponse(data)

        # Mocks
        feature_store.ingest = MagicMock()
        feature_store._ingest_files = MagicMock()

        # Test
        feature_store.ingest_response(feature_table, read_response)

        # Assertions
        feature_store.ingest.assert_called_with(feature_table, data, None, None)
        feature_store._ingest_files.assert_not_called()

    @patch.object(FeatureStore, "_has_medias", return_value=True)
    @patch.object(FeatureStore, "ingest")
    def test_ingest_response_no_rename_no_filter_binary_upload(self, mock_ingest, mock_has_medias):
        # Setup
        feast.FeatureStore = MagicMock()
        feature_store = FeatureStore()
        feature_table = FeatureTable(
            name="test_table",
            feature_store=feast.FeatureStore(),
            features=[
                feast.Feature(name="feature1", dtype=feast.ValueType.INT64),
                feast.Feature(name="media", dtype=feast.ValueType.STRING),
            ],
        )

        data = pd.DataFrame(
            {
                "feature1": [1, 2, 3],
                "media": ["path1", "path2", "pat3"],
                "event_timestamp": [datetime.utcnow()] * 3,
                "created_timestamp": [datetime.utcnow()] * 3,
            }
        )

        read_response = ReadResponse(data)

        # Mocks
        feature_store.ingest = MagicMock()
        feature_store._ingest_files = MagicMock()

        # Test
        feature_store.ingest_response(feature_table, read_response)

        # Assertions
        feature_store.ingest.assert_called_with(feature_table, data, None, None)
        feature_store._ingest_files.assert_called_with(read_response)

    def test_ingest_response_with_renames(self):
        # Setup
        feast.FeatureStore = MagicMock()
        feature_store = FeatureStore()
        feature_table = FeatureTable(
            name="test_table",
            feature_store=feast.FeatureStore(),
            features=[
                feast.Feature(name="feature1", dtype=feast.ValueType.INT64),
                feast.Feature(name="feature2", dtype=feast.ValueType.STRING),
            ],
        )

        data = pd.DataFrame(
            {
                "old_feature1": [1, 2, 3],
                "old_feature2": ["a", "b", "c"],
                "event_timestamp": [datetime.utcnow()] * 3,
                "created_timestamp": [datetime.utcnow()] * 3,
            }
        )

        read_response = ReadResponse(data)

        renames = {
            "old_feature1": "feature1",
            "old_feature2": "feature2",
        }

        # Mocks
        feature_store.ingest = MagicMock()

        # Test
        feature_store.ingest_response(feature_table, read_response, renames=renames)

        # Assertions
        called_args, called_kwargs = feature_store.ingest.call_args
        called_dataframe = called_args[1]

        pd.testing.assert_frame_equal(called_dataframe, data)
        feature_store.ingest.assert_called_with(feature_table, data, renames, None)

    def test_ingest_response_with_column_filtering(self):
        # Setup
        feast.FeatureStore = MagicMock()
        feature_store = FeatureStore()
        feature_table = FeatureTable(
            name="test_table",
            feature_store=feast.FeatureStore(),
            features=[
                feast.Feature(name="feature1", dtype=feast.ValueType.INT64),
                feast.Feature(name="feature2", dtype=feast.ValueType.STRING),
            ],
        )

        data = pd.DataFrame(
            {
                "feature1": [1, 2, 3],
                "feature2": ["a", "b", "c"],
                "extra_column": [4, 5, 6],
                "event_timestamp": [datetime.utcnow()] * 3,
                "created_timestamp": [datetime.utcnow()] * 3,
            }
        )

        read_response = ReadResponse(data)

        all_columns = ["feature1", "feature2", "event_timestamp", "created_timestamp"]

        # Mocks
        feature_store.ingest = MagicMock()

        # Test
        feature_store.ingest_response(feature_table, read_response, all_columns=all_columns)

        # Assertions
        called_args, called_kwargs = feature_store.ingest.call_args
        called_dataframe = called_args[1]

        pd.testing.assert_frame_equal(called_dataframe, data)
        feature_store.ingest.assert_called_with(feature_table, data, None, all_columns)


if __name__ == "__main__":
    unittest.main()
