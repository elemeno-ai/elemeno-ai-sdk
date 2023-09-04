import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from elemeno_ai_sdk.ml.features.ingest.sink.minio_ingestion import MediaColumn, MinioIngestion


class TestMinioIngestion(unittest.TestCase):
    @patch("elemeno_ai_sdk.ml.features.ingest.sink.minio_ingestion.Configs")
    def setUp(self, mock_configs):
        # Mock Configs
        config_instance = mock_configs.instance.return_value
        config_instance.cos.host = "fake_host"
        config_instance.cos.key_id = "fake_key_id"
        config_instance.cos.secret = "fake_secret"
        config_instance.cos.use_ssl = False
        config_instance.cos.bucket = "fake_bucket"
        self.minio_ingestion = MinioIngestion()

    def test_io_batch_digest_empty_dataframe(self):
        feature_table_name = "test_feature_table"
        to_digest = pd.DataFrame()
        media_columns = [MediaColumn(name="media_col_1", is_upload=True)]

        # No exception should be raised when passing an empty DataFrame
        try:
            self.minio_ingestion.io_batch_digest(feature_table_name, to_digest, media_columns)
        except Exception as e:
            self.fail(f"An exception occurred while processing an empty DataFrame: {e}")

    @patch("elemeno_ai_sdk.ml.features.ingest.sink.minio_ingestion.cpu_count")
    @patch("elemeno_ai_sdk.ml.features.ingest.sink.minio_ingestion.Pool")
    def test_io_batch_digest_single_media_file_single_column(self, mock_pool, mock_cpu_count):
        # Mock cpu_count
        mock_cpu_count.return_value = 1

        # Mock Pool
        pool_instance = mock_pool.return_value
        pool_instance.map.return_value = None
        pool_instance.close.return_value = None
        pool_instance.join.return_value = None

        feature_table_name = "test_feature_table"
        to_digest = pd.DataFrame({"media_col_1": ["file_path_1"]})
        media_columns = [MediaColumn(name="media_col_1", is_upload=True)]

        # No exception should be raised when processing a single media file
        try:
            self.minio_ingestion.io_batch_digest(feature_table_name, to_digest, media_columns)
        except Exception as e:
            self.fail(f"An exception occurred while processing a single media file: {e}")

        # Check if the mocked methods were called with the expected arguments
        mock_pool.assert_called_once()
        pool_instance.map.assert_called_once()

    @patch("elemeno_ai_sdk.ml.features.ingest.sink.minio_ingestion.cpu_count")
    @patch("elemeno_ai_sdk.ml.features.ingest.sink.minio_ingestion.Pool")
    def test_io_batch_digest_multiple_media_files_single_column(self, mock_pool, mock_cpu_count):
        # Mock cpu_count
        mock_cpu_count.return_value = 1

        # Mock Pool
        pool_instance = mock_pool.return_value
        pool_instance.map.return_value = None
        pool_instance.close.return_value = None
        pool_instance.join.return_value = None

        feature_table_name = "test_feature_table"
        to_digest = pd.DataFrame({"media_col_1": ["file_path_1", "file_path_2", "file_path_3"]})
        media_columns = [MediaColumn(name="media_col_1", is_upload=False)]

        # No exception should be raised when processing multiple media files
        try:
            self.minio_ingestion.io_batch_digest(feature_table_name, to_digest, media_columns)
        except Exception as e:
            self.fail(f"An exception occurred while processing multiple media files: {e}")

        # Check if the mocked methods were called with the expected arguments
        mock_pool.assert_called_once()
        pool_instance.map.assert_called_once()

    @patch("elemeno_ai_sdk.ml.features.ingest.sink.minio_ingestion.cpu_count")
    @patch("elemeno_ai_sdk.ml.features.ingest.sink.minio_ingestion.Pool")
    def test_io_batch_digest_media_files_multiple_columns(self, mock_pool, mock_cpu_count):
        # Mock cpu_count
        mock_cpu_count.return_value = 1

        # Mock Pool
        pool_instance = mock_pool.return_value
        pool_instance.map.return_value = None
        pool_instance.close.return_value = None
        pool_instance.join.return_value = None

        feature_table_name = "test_feature_table"
        to_digest = pd.DataFrame(
            {"media_col_1": ["file_path_1", "file_path_2"], "media_col_2": ["file_path_3", "file_path_4"]}
        )
        media_columns = [
            MediaColumn(name="media_col_1", is_upload=False),
            MediaColumn(name="media_col_2", is_upload=True),
        ]

        # No exception should be raised when processing media files from multiple MediaColumns
        try:
            self.minio_ingestion.io_batch_digest(feature_table_name, to_digest, media_columns)
        except Exception as e:
            self.fail(f"An exception occurred while processing media files from multiple MediaColumns: {e}")

        # Check if the mocked methods were called with the expected arguments
        self.assertEqual(mock_pool.call_count, len(media_columns))
        self.assertEqual(pool_instance.map.call_count, len(media_columns))

    @patch("elemeno_ai_sdk.ml.features.ingest.sink.minio_ingestion.cpu_count")
    @patch("elemeno_ai_sdk.ml.features.ingest.sink.minio_ingestion.Pool")
    def test_io_batch_digest_invalid_media_file_paths(self, mock_pool, mock_cpu_count):
        # Mock cpu_count
        mock_cpu_count.return_value = 1

        # Mock Pool
        pool_instance = mock_pool.return_value

        def mock_map(func, iterable):
            for item in iterable:
                try:
                    func(item)
                except Exception:
                    pass

        pool_instance.map.side_effect = mock_map
        pool_instance.close.return_value = None
        pool_instance.join.return_value = None

        # Mock download_file_from_remote to raise an exception for invalid file paths
        def mock_download_file_from_remote(*args, **kwargs):
            raise Exception("Invalid file path")

        self.minio_ingestion.download_file_from_remote = MagicMock(side_effect=mock_download_file_from_remote)

        feature_table_name = "test_feature_table"
        to_digest = pd.DataFrame({"media_col_1": ["invalid_file_path_1", "invalid_file_path_2"]})
        media_columns = [MediaColumn(name="media_col_1", is_upload=True)]

        # No exception should be raised when processing invalid media file paths
        try:
            self.minio_ingestion.io_batch_digest(feature_table_name, to_digest, media_columns)
        except Exception as e:
            self.fail(f"An exception occurred while processing invalid media file paths: {e}")

        # Check if the mocked methods were called with the expected arguments
        mock_pool.assert_called_once()
        pool_instance.map.assert_called_once()


if __name__ == "__main__":
    unittest.main()
