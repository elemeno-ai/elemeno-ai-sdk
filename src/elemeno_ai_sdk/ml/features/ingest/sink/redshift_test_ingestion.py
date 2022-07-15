
from elemeno_ai_sdk.ml.features.ingest.sink.redshift_ingestion import RedshiftIngestion


class RedshiftTestIngestion(RedshiftIngestion):
    def __init__(self, fs, connection_string: str):
      super().__init__(fs, connection_string=connection_string)
      self.rs_types = {
        "object": "BLOB",
        "string[python]": "TEXT",
        "string": "TEXT",
        "Int64": "INTEGER",
        "Int32": "INTEGER",
        "Float64": "REAL",
        "bool": "INTEGER",
        "datetime64[ns]": "TEXT"
      }