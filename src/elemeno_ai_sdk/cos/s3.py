from typing import Any, Optional, List
import boto3
from botocore.client import ClientError
from elemeno_ai_sdk.cos.base_cos import CloudObjectStorage
from elemeno_ai_sdk.cos.types import *
from elemeno_ai_sdk.config import Configs
from elemeno_ai_sdk import logger

class S3Client(CloudObjectStorage):

    def __init__(self, access_key: Optional[str] = None, 
        secret_key: Optional[str] = None,
        session_token: Optional[str] = None) -> None:
      """
      S3Client is a cloud object storage to use when interacting with AWS S3.

      args:

      - access_key: AWS access key
      - secret_key: AWS secret key
      - session_token: AWS session token, this is optional. Only useful when using temporary credentials.
      """
      
      cfg = Configs.instance()
      self._config = cfg
      if access_key is None:
        access_key = cfg.cos.key_id
      if secret_key is None:
        secret_key = cfg.cos.secret
      if session_token is None:
        if hasattr(cfg.cos, 'session_token'):
          session_token = cfg.cos.session_token

      self.client = boto3.client('s3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token)
    
    def list_dir(self, bucket_name: str, prefix: str = "") -> List[CosObject]:
      """
      List all objects in a bucket. Filtering by prefix if specified.

      args:

      - bucket_name: name of the bucket
      - prefix: prefix to filter objects by
      """
      self._bucket_exists(bucket_name)
      bucket = self.client.Bucket(bucket_name)
      ext_objs = bucket.objects.all()
      for eo in ext_objs:
        yield self.to_cos_object(eo)

    def get_object(self, bucket_name: str, object_path: str, destination_path: str):
      """
      Download an object from a bucket.

      args:

      - bucket_name: name of the bucket
      - object_path: path to the object in the bucket
      - destination_path: path to save the object to, including the filename
      """
      self._bucket_exists(bucket_name)
      resp = self.client.get_object(Bucket=bucket_name, Key=object_path)
      with open(destination_path, 'wb') as f:
        f.write(resp['Body'].read())

    def put_object(self, bucket_name: str, destination_path: str, object_stream: Any):
      """
      Upload an object to a bucket.

      args:

      - bucket_name: name of the bucket
      - destination_path: path to save the object to, including the filename
      - object_stream: stream of the object to upload, it needs to be of a type that contains a read() method returning bytes
      """
      self._bucket_exists(bucket_name)
      self.client.put_object(Bucket=bucket_name, Key=destination_path, Body=object_stream.read())
    
    def to_cos_object(self, external_object: Any) -> CosObject:
       return CosObject(
        bucket_name=external_object.bucket_name,
        object_path=external_object.key,
        size=external_object.size,
        last_modified=external_object.last_modified,
        etag=external_object.e_tag,
        storage_class=external_object.storage_class,
        owner=external_object.owner
       )
    
    def _bucket_exists(self, bucket_name: str):
      try:
        self.client.head_bucket(Bucket=bucket_name)
      except ClientError:
        logger.debug(f"Bucket {bucket_name} doesn't exist")
        raise ValueError("The specified bucket doesn't exist")

