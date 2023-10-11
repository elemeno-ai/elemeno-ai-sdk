import json
import pprint
from typing import Any, Dict, List, NamedTuple, Optional

import marshmallow
from marshmallow import Schema, ValidationError, fields, validates


class ValidationReport(NamedTuple):
    """Container for storing validation test results.
    Attributes:
        report_type: The name of the report type.
        report: The object storing report data.
        pformatted_report: The pretty print formatted string serialization of the
           report.
    """

    report_type: str
    report: dict
    pformatted_report: str


class Features(Schema):
    name = fields.String(required=True)
    type = fields.String(required=True)

    @validates(field_name="name")
    def is_lower(self, name: str) -> None:
        if not name.islower():
            raise ValidationError(message=f"Feature {name} is in uppercase. Feature names must be lowercase")


class FeatureTableSchema(Schema):
    name = fields.String(required=True)
    entities = fields.List(fields.String(), required=True)
    schema = fields.List(fields.Nested(Features, required=True), required=True)

    @validates(field_name="schema")
    def check_if_event_timestamp_is_in_schema(self, schema: List[Features]):
        feature_names = [feat["name"] for feat in schema]
        if "event_timestamp" not in feature_names:
            raise ValidationError(message="Event timestamp must be in your feature table shema with type 'timestamp'")

    def load_data(self, schema_path: str) -> Dict[str, Any]:
        with open(schema_path, "r") as schema:
            table_schema = json.load(schema)

        schema_validation_report = self._validate(table_schema=table_schema)
        if schema_validation_report is not None:
            return schema_validation_report

        return self.load(data=table_schema, unknown=marshmallow.RAISE)

    def _validate(self, table_schema: Dict[str, Any]) -> Optional[ValidationReport]:
        schema_validation_failures = self.validate(data=table_schema)
        if schema_validation_failures:
            schema_validation_report = ValidationReport(
                report_type="Schema Validation",
                report=schema_validation_failures,
                pformatted_report=pprint.PrettyPrinter().pformat(schema_validation_failures),
            )
            return schema_validation_report
