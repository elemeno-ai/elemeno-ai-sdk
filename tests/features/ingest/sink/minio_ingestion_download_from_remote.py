import unittest
from unittest.mock import patch, MagicMock, Mock
from elemeno_ai_sdk.ml.features.ingest.sink.minio_ingestion import DigestionParams, MinioIngestion
from elemeno_ai_sdk.config import Configs
from elemeno_ai_sdk.cos.minio import MinioClient
from minio.error import S3Error
from omegaconf import OmegaConf

class TestMinioIngestion(unittest.TestCase):

  def setupConfig(self):
    c = OmegaConf.create({
        "cos": {
            "host": "fake_host",
            "key_id": "fake_key_id",
            "secret": "fake_secret",
            "use_ssl": False,
            "bucket": "fake_bucket"
        }
    })
    return c
  

  def test_download_file_from_remote_success(self):
    test_config = self.setupConfig()
    
    with patch('elemeno_ai_sdk.config.Configs.instance', return_value=test_config):
      # Mock MinioClient
      minio_client_mock = MagicMock(spec=MinioClient)
      minio_client_mock.bucket_exists.return_value = True

      # Mock MinioIngestion
      minio_ingestion = MinioIngestion()

      with patch("elemeno_ai_sdk.ml.features.ingest.sink.minio_ingestion.minio_client") as minio_client:
        minio_client.return_value = minio_client_mock

        # Define DigestionParams
        digestion_params = DigestionParams(
          media_path_col="media_path",
          to_digest={"media_path": "test_file.txt"},
          remote_folder="remote_folder"
        )

        # Call the method
        minio_ingestion.download_file_from_remote(digestion_params)

        # Assert that the bucket_exists and get_object methods were called
        minio_client_mock.bucket_exists.assert_called_once_with("elemeno-cos")
        minio_client_mock.get_object.assert_called_once_with(
          "elemeno-cos", "remote_folder/test_file.txt", ".binaries/test_file.txt"
        )

  def test_download_file_from_remote_invalid_file_path(self):
    # Setup
    test_config = self.setupConfig()
    with patch('elemeno_ai_sdk.config.Configs.instance', return_value=test_config), \
            patch('elemeno_ai_sdk.cos.minio.MinioClient.bucket_exists') as existsmock, \
            patch('elemeno_ai_sdk.cos.minio.MinioClient.get_object') as fget_object_mock, \
            patch('logging.error') as log_error_mock:

        existsmock.return_value = True
        s3_error = S3Error("NoSuchKey", "Object not found", resource="non_existent.txt", request_id="12345", host_id="12345", response="12345")
        fget_object_mock.side_effect = Mock(side_effect=s3_error)

        ingestion = MinioIngestion()
        params = DigestionParams(media_path_col='file_path', to_digest={'file_path': 'non_existent.txt'}, remote_folder='test_folder')

        # Execute
        ingestion.download_file_from_remote(params)

        # Assert
        log_error_mock.assert_called_with(f'Error downloading file non_existent.txt from bucket elemeno-cos and folder test_folder. Error: S3 operation failed; code: NoSuchKey, message: Object not found, resource: non_existent.txt, request_id: 12345, host_id: 12345')
  
  def test_download_file_from_remote_invalid_bucket(self):
    # Setup
    test_config = self.setupConfig()
    with patch('elemeno_ai_sdk.config.Configs.instance', return_value=test_config), \
      patch('elemeno_ai_sdk.cos.minio.MinioClient.bucket_exists') as existsmock:

      existsmock.return_value = False
      ingestion = MinioIngestion()
      params = DigestionParams(media_path_col='file_path', to_digest={'file_path': 'test.txt'}, remote_folder='test_folder', minio_bucket='invalid_bucket')

      # Execute and assert
      with self.assertRaises(Exception):
        ingestion.download_file_from_remote(params)

  @patch('os.makedirs')
  @patch('os.path.exists')
  def test_local_folder_creation(self, mock_exists, mock_makedirs):
    # Setup
    test_config = self.setupConfig()
    with patch('elemeno_ai_sdk.config.Configs.instance', return_value=test_config), \
      patch('elemeno_ai_sdk.cos.minio.MinioClient.bucket_exists') as existsmock:
      
      existsmock.return_value = True
      mock_exists.return_value = False
      ingestion = MinioIngestion()
      params = DigestionParams(media_path_col='file_path', to_digest={'file_path': 'test.txt'}, remote_folder='test_folder')

      # Execute
      ingestion.download_file_from_remote(params)

      # Assert
      mock_makedirs.assert_called_once_with('.binaries')

if __name__ == '__main__':
  unittest.main()
