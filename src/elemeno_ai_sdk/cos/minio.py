from minio import Minio
from minio.error import S3Error
from elemeno_ai_sdk.config import Configs

class MinioClient:

    def __init__(self):
        cfg = Configs.instance()
        self._config = cfg
        self.client = Minio(
            cfg.cos.host,
            access_key=cfg.cos.key_id,
            secret_key=cfg.cos.secret,
            secure=cfg.cos.use_ssl
        )

    def get_object(self, bucket_name: str, object_path: str, destination_path: str):
        found = self.client.bucket_exists(bucket_name)
        if not found:
            raise ValueError("The specified bucket doesn't exist")
        self.client.fget_object(bucket_name, object_path, destination_path)

