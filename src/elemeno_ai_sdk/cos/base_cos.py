import abc

from typing import Any, List, Optional
from elemeno_ai_sdk.cos.types.cos_object import CosObject

class CloudObjectStorage(abc.ABC):

  def __init__(self, **kwargs):
    self.cos_type = type(self).__name__

  @abc.abstractmethod
  def list_dir(self, bucket_name: str, prefix: str = "") -> List[CosObject]:
    pass

  @abc.abstractmethod
  def get_object(self, bucket_name: str, object_path: str, destination_path: str):
    pass

  @abc.abstractmethod
  def put_object(self, bucket_name: str, destination_path: str, object_stream: Any, length: Optional[int] = -1):
    pass

  @abc.abstractmethod
  def to_cos_object(self, external_object: Any) -> CosObject:
    pass