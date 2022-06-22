from typing import Any, Optional
from minio import Minio
from minio.error import S3Error
from elemeno_ai_sdk.config import Configs

class MinioClient:

    def __init__(self, host: Optional[str] = None, access_key: Optional[str] = None, 
        secret_key: Optional[str] = None, use_ssl: Optional[bool] = None) -> None:
      cfg = Configs.instance()
      self._config = cfg
      if host is None:
        host = cfg.cos.host
      if access_key is None:
        access_key = cfg.cos.key_id
      if secret_key is None:
        secret_key = cfg.cos.secret
      if use_ssl is None:
        use_ssl = cfg.cos.use_ssl

      self.client = Minio(
          host,
          access_key=access_key,
          secret_key=secret_key,
          secure=use_ssl
      )
    
    def list_dir(self, bucket_name: str, prefix: str = "") -> list:
        found = self.client.bucket_exists(bucket_name)
        if not found:
            raise ValueError("The specified bucket doesn't exist")
        return self.client.list_objects(bucket_name, prefix=prefix)

    def get_object(self, bucket_name: str, object_path: str, destination_path: str):
        found = self.client.bucket_exists(bucket_name)
        if not found:
            raise ValueError("The specified bucket doesn't exist")
        self.client.fget_object(bucket_name, object_path, destination_path)

    def put_object(self, bucket_name: str, destination_path: str, object_stream: Any, length: Any = -1):
        found = self.client.bucket_exists(bucket_name)
        if not found:
            raise ValueError("The specified bucket doesn't exist")
        self.client.put_object(bucket_name, destination_path, object_stream, length, part_size=5242880)

