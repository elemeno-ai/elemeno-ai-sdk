
import enum
from typing import Any, Dict, List

class FeatureMeaning(enum.Enum):
  SEARCH_KEY = "SEARCH_KEY"
  FEATURE = "FEATURE"
  BODY_FEATURE = "BODY_FEATURE"

  """
  Enum for the meaning of a feature
  Valid meaning values are:
    - SEARCH_KEY - to be used as the key when Elemeno search for your features in the feature store
    - FEATURE - the name of features to be found using the SEARCH_KEY specified
    - BODY_FEATURE - a dynamic feature that will not be retrieved from the feature store, 
    it will be added to the request body instead
  """

  def from_str(self, meaning: str) -> "FeatureMeaning":
    if meaning == 'SEARCH_KEY':
      return FeatureMeaning.SEARCH_KEY
    elif meaning == 'FEATURE':
      return FeatureMeaning.FEATURE
    elif meaning == 'BODY_FEATURE':
      return FeatureMeaning.BODY_FEATURE

class InputSpace:
  def __init__(self, entities: Dict[str, Any]):
    self._entities = entities
    pass

  @property
  def entities(self) -> Dict[str, Any]:
    return self._entities
  
  @entities.setter
  def entities(self, entities: Dict[str, Any]) -> None:
    self._entities = entities
  
class InputSpaceBuilder:

  def __init__(self):
    self._entities = {}
    pass
  
  def with_entities(self, entities: Dict[str, Any]) -> "InputSpaceBuilder":
    self._entities = entities
    return self
  
  def build(self) -> InputSpace:
    return InputSpace(self._entities)

class FeatureReference:

  def __init__(self, meaning: FeatureMeaning, name: str):
    pass

  @property
  def meaning(self) -> FeatureMeaning:
    return self.meaning
  
  @meaning.setter
  def meaning(self, meaning: FeatureMeaning) -> None:
    self.meaning = meaning
  
  @property
  def name(self) -> str:
    return self.name
  
  @name.setter
  def name(self, name: str) -> None:
    self.name = name


