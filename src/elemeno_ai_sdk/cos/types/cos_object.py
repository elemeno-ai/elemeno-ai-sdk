
from typing import Optional

class CosObject:

  """Class representing a COS object.

  This class is used to represent a COS object. It is used to represent the remote object in S3, minio, gcs, etc.
  """

  def __init__(self, bucket_name: str, object_path: str, size: int, last_modified: str, 
      etag: Optional[str] = None, storage_class: Optional[str] = None, owner: Optional[str] = None) -> None:
    self.bucket_name = bucket_name
    self.object_path = object_path
    self.size = size
    self.last_modified = last_modified
    self.etag = etag
    self.storage_class = storage_class
    self.owner = owner

  def __str__(self) -> str:
    return f"bucket_name: {self.bucket_name}, object_path: {self.object_path}, size: {self.size}, last_modified: {self.last_modified}, etag: {self.etag}, storage_class: {self.storage_class}, owner: {self.owner}"

  def __eq__(self, o: object) -> bool:
    if not isinstance(o, CosObject):
      return False
    return self.bucket_name == o.bucket_name and self.object_path == o.object_path and self.size == o.size and self.last_modified == o.last_modified and self.etag == o.etag and self.storage_class == o.storage_class and self.owner == o.owner

  def __ne__(self, o: object) -> bool:
    return not self.__eq__(o)

  def __hash__(self) -> int:
    return hash((self.bucket_name, self.object_path, self.size, self.last_modified, self.etag, self.storage_class, self.owner))