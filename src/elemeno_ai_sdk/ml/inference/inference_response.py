
from typing import Any, Dict, List


class InferenceResponse:

  def __init__(self) -> None:
      pass
  
  @property
  def output(self) -> List[Dict[str, Any]]:
    return self._output
  
  @output.setter
  def set_output(self, output: List[Dict[str, Any]]) -> None:
    self._output = output
  
  def pretty_print(self) -> None:
    for entity in self.output:
      print(entity)
      for key, value in entity.items():
        print(f'{key}: {value}')
