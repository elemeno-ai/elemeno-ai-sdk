import unittest
from unittest.mock import patch

from elemeno_ai_sdk.ml.features.feature_store import FeatureStore


class TestFeatureStore(unittest.TestCase):
    @patch("feast.FeatureStore")
    def test_init_missing_config(self, mock_feast_feature_store):
        with self.assertRaises(Exception) as context:
            FeatureStore()

        self.assertIn(
            "Missing elemeno.yaml file. Make sure it's in the current directory or in the ELEMENO_CFG_FILE environment variable.",
            str(context.exception),
        )


if __name__ == "__main__":
    unittest.main()
