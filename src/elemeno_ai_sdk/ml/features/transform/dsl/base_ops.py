from typing import Dict
from elemeno_ai_sdk.ml.features.transform.dsl.functions import *

class BaseOps:

  def __init__(self) -> None:
      """
        Just an object to define all base operations available at the DSL level.
      """
      pass
  
  @property
  def source(self) -> str:
    return self._source
  
  @source.setter
  def set_source(self, source: str) -> None:
    self._source = source
  
  @property
  def output(self) -> str:
    return self._output
  
  @output.setter
  def set_output(self, output: str) -> None:
    self._output = output
  
  # @property
  # def pipeline(self) -> :
  
  @property
  def change_shape(self) -> "ChangeShapeFn":
    return ChangeShapeFn.parse_from_yaml(self._change_shape)
  
  def set_change_shape(self, change_shape: Dict) -> None:
    self._change_shape = change_shape

  # @staticmethod
  # def parse_from_yaml(yaml_data: Dict) -> "BaseOps":
