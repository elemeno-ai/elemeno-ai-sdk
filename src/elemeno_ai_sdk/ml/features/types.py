import enum
from typing import Any, Optional
import numpy as np
import feast
import pandas as pd

class BqType(enum.Enum):
    """
    BigQuery enum types. Used when dealing with types in any big query operation.
    """

    FLOAT = 0
    STRING = 1
    DATETIME = 2
    TIMESTAMP = 3
    ARRAY = 4
    BOOL = 5
    STRUCT = 6
    INTEGER = 7
class FeatureType:
    """
    Feature value type. Used to define data types in Feature Tables.
    Inherited from Feast
    """
    
    @staticmethod
    def from_feast_to_json_schema_feast(field_type: str) -> str:
        if field_type.lower() == "struct":
          return "string"
        return field_type.lower()

    def from_str_to_bq_type(type_in_str: str, format: Optional[str] = None) -> BqType:
        """Converts the string containing the type (JSONSchema types are supported)
        to a Big Query equivalent type. Returns a BqType Enum value

        Keyword arguments:
        type_in_str -- the type name (JSONSchema)
        """
        if type_in_str == "number":
            return BqType.FLOAT
        elif type_in_str == "string":
            if format != None and format == "date-time":
                return BqType.TIMESTAMP
            return BqType.STRING
        elif type_in_str == "array":
            return BqType.ARRAY
        elif type_in_str == "object":
            return BqType.STRUCT
        elif type_in_str == "boolean":
            return BqType.BOOL
        elif type_in_str == "integer":
            return BqType.INTEGER
        elif type_in_str == "binary_download":
            return BqType.STRING
        else:
            raise ValueError("Unsupported type in bigquery")

    def from_str_to_pd_type(type_in_str: str, format: Optional[str] = None) -> np.dtype:
        """Converts the string containing the type (JSONSchema types are supported)
        to a pandas equivalent type. Returns a np.dtype

        Keyword arguments:
        type_in_str -- the type name (JSONSchema)
        """
        if type_in_str == "number":
            return np.float64
        elif type_in_str == "string":
            if format != None and format == "date-time":
                return np.datetime64('2002-02-03T13:56:03.172')
            return np.unicode_
        elif type_in_str == "array":
            return pd.Series([]).dtype
        elif type_in_str == "object":
            return np.bytes_
        elif type_in_str == "boolean":
            return np.bool_
        elif type_in_str == "integer":
            return np.int32
        elif type_in_str == "binary_download":
            return np.bytes_
        else:
            raise ValueError("Unsupported type in pandas")

    def from_str_to_entity_type(type_in_str: str) -> feast.ValueType:
        """Converts the string containing the type (JSONSchema types are supported)
        to a Feast ValueType to be used in the FeatureStore. Returns a ValueType from feast-sdk

        Keyword arguments:
        type_in_str -- the type name (JSONSchema)
        """
        if type_in_str == "number":
            return feast.ValueType.FLOAT
        elif type_in_str == "string":
            return feast.ValueType.STRING
        elif type_in_str in ["array", "object"]:
            return feast.ValueType.BYTES
        elif type_in_str == "boolean":
            return feast.ValueType.BOOL
        elif type_in_str == "integer":
            return feast.ValueType.INT32
        elif type_in_str == "binary_download":
            return feast.ValueType.STRING
        else:
            return feast.ValueType.UNKNOWN

    def from_str_to_feature_type(type_in_str: str) -> feast.ValueType:
      """Converts the string containing the type (JSONSchema types are supported)
      to a Feast ValueType to be used in the FeatureStore. Returns a ValueType from feast-sdk

      Keyword arguments:
      type_in_str -- the type name (JSONSchema)
      """
      if type_in_str == "number":
          return feast.types.PrimitiveFeastType.FLOAT64
      elif type_in_str == "string":
          return feast.types.PrimitiveFeastType.STRING
      elif type_in_str in ["array", "object"]:
          return feast.types.PrimitiveFeastType.BYTES
      elif type_in_str == "boolean":
          return feast.types.PrimitiveFeastType.BOOL
      elif type_in_str == "integer":
          return feast.types.PrimitiveFeastType.INT32
      elif type_in_str == "binary_download":
          return feast.types.PrimitiveFeastType.STRING
      else:
          return feast.types.PrimitiveFeastType.UNKNOWN
    
    @staticmethod
    def get_dummy_value(dtype: np.dtype) -> Any:
      """
      Returns a dummy value for the given dtype.
      """
      if not hasattr(dtype, '__name__'):
        return dtype
      if dtype.__name__ == np.float64.__name__:
        return np.float64(1.50)
      elif dtype.__name__ == np.int32.__name__:
        return np.int32(1)
      elif dtype.__name__ == np.bool_.__name__:
        return np.bool_(True)
      elif dtype.__name__ == np.bytes_.__name__:
        return np.bytes_(b'bytes')
      elif dtype.__name__ == np.unicode_.__name__:
        return np.unicode_('string')
      elif dtype.__name__ == np.ndarray.__name__ or dtype.__name__ == np.object_.__name__:
        return np.array([1, 2, 3])
      else:
        raise ValueError("Unsupported dtype")
