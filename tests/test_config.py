import unittest
from importlib import reload
from unittest.mock import patch

import elemeno_ai_sdk.config


class TestConfigs(unittest.TestCase):
    @patch.dict("os.environ", {"ELEMENO_CFG_FILE": "tests/elemeno.yaml"})
    def test_instance(self):
        from elemeno_ai_sdk.config import Configs

        configs = Configs.instance()
        self.assertIsNotNone(configs)

    @patch.dict("os.environ", {"ELEMENO_CFG_FILE": "non_existent.yaml"})
    def test_missing_file_warn(self):
        with self.assertLogs(level="WARNING") as cm:
            reload(elemeno_ai_sdk.config)
            from elemeno_ai_sdk.config import Configs

            configs = Configs.instance()
            self.assertIsNone(configs)
            self.assertIn("Couldn't find a config file at", cm.output[0])


if __name__ == "__main__":
    unittest.main()
