import enum
import feast

class FeatureType:
    """
    Feature value type. Used to define data types in Feature Tables.
    Inherited from Feast
    """

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
        else:
            return feast.ValueType.UNKNOWN
