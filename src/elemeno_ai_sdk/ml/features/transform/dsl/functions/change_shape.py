
from typing import Dict


class ChangeShapeFn:

  def __init__(self, source_data: Dict, instructions: Dict) -> None:
    """
      This class is the object representing a ChangeShape function specified via yaml
      An example of this function definition in yaml is:
        source: "memory"
        output: "memory"
        pipeline:
        - change_shape:
            instructions:
              - type: rename_column
                dimension: 0
                field: "x"
                new_name: "renamed_x"
              - type: create_field
                dimension: 0
                field: "y"
                default_value: 0
              - type: nest_field
                dimension: 0
                field: "z"
                new_name: "nested_z"
                
    """
    self.source_data = source_data
    self.instructions = instructions

  SUPPORTED_INSTRUCTIONS = ["rename_column", "create_field", "nest_field"]

  def apply(self) -> Dict:
    for instruction in self.instructions:
      dimension = instruction["dimension"]
      field = instruction["field"]
      if instruction["type"] == "rename_column":
        new_name = instruction["new_name"]
        self.source_data[dimension][new_name] = self.source_data[dimension][field]
        del self.source_data[dimension][field]
      elif instruction["type"] == "create_field":
        default_value = instruction["default_value"]
        self.source_data[dimension][field] = default_value
      elif instruction["type"] == "nest_field":
        new_name = instruction["new_name"]
        self.source_data[dimension][new_name] = self.source_data[dimension][field]
        del self.source_data[dimension][field]
    return self.source_data
  
  @staticmethod
  def parse_from_yaml(self, yaml_data: Dict) -> "ChangeShapeFn":
    instructions = yaml_data["change_shape"]["instructions"]
    for instruction in instructions:
      if instruction["type"] not in ChangeShapeFn.SUPPORTED_INSTRUCTIONS:
        raise ValueError(f"Unsupported instruction type: {instruction['type']}")
    return ChangeShapeFn(yaml_data, instructions)