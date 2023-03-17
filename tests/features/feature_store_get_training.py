import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
import pandas as pd
import feast
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable
from elemeno_ai_sdk.ml.features.feature_store import FeatureStore

@patch.dict('os.environ', {'ELEMENO_CFG_FILE': 'tests/elemeno.yaml'})
class TestFeatureStore(unittest.TestCase):

  def test_get_training_features_download_files(self):
    # Prepare test data and mocks
    feature_table = FeatureTable(
      name="test_table",
      feature_store=feast.FeatureStore(repo_path="."),
      entities=[feast.Entity(name="entity_id", value_type=feast.ValueType.INT64)],
      features=[feast.Feature(name="feature_1", dtype=feast.ValueType.INT64), feast.Feature(name="feature_2", dtype=feast.ValueType.INT64)]
    )
    feature_store = FeatureStore()
    feature_store._sink = MagicMock()
    feature_store._file_sink = MagicMock()
    feature_store._has_medias = MagicMock(return_value=True)

    test_df = pd.DataFrame({
      "created_timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 2)],
      "entity_id": [1, 2],
      "feature_1": [1, 2],
      "feature_2": [3, 4],
      "media_column": ["path/to/media1", "path/to/media2"]
    })

    feature_store._sink.read_table.return_value = test_df

    # Call the method
    result_df = feature_store.get_training_features(
      feature_table=feature_table,
      download_binaries=True
    )

    # Assertions
    feature_store._sink.read_table.assert_called_once()
    feature_store._file_sink.io_batch_digest.assert_called_once_with(
      feature_table.name, test_df, feature_store._media_columns(feature_table)
    )
    self.assertTrue("path/to/media1" in result_df["media_column"].values)
    self.assertTrue("path/to/media2" in result_df["media_column"].values)

if __name__ == '__main__':
  unittest.main()
