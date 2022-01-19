import enum
from typing import Optional
import numpy as np
import feast

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
            return np.ndarray
        elif type_in_str == "object":
            return np.bytes_
        elif type_in_str == "boolean":
            return np.bool_
        elif type_in_str == "integer":
            return np.int32
        else:
            raise ValueError("Unsupported type in pandas")

    def from_str_to_feature_type(type_in_str: str) -> feast.ValueType:
        """Converts the string containing the type (JSONSchema types are supported)
        to a Feast ValueType to be used in the FeatureStore. Returns a ValueType from feast-sdk

        Keyword arguments:
        type_in_str -- the type name (JSONSchema)
        """
        if type_in_str == "number":
            return feast.ValueType.DOUBLE
        elif type_in_str == "string":
            return feast.ValueType.STRING
        elif type_in_str in ["array", "object"]:
            return feast.ValueType.BYTES
        elif type_in_str == "boolean":
            return feast.ValueType.BOOL
        elif type_in_str == "integer":
            return feast.ValueType.INT32
        else:
            return feast.ValueType.UNKNOWN
