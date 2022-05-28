
import enum
from elemeno_ai_sdk.ml.features.ingest.sink.base_ingestion import Ingestion

from elemeno_ai_sdk.ml.features.ingest.sink.bigquery_ingestion import BigQueryIngestion
from elemeno_ai_sdk.ml.features.ingest.sink.redshift_ingestion import RedshiftIngestion

class IngestionSinkType(enum.Enum):
  BIGQUERY = "BigQuery"
  REDSHIFT = "Redshift"

class IngestionSinkBuilder:
  
  def build_bigquery(self, fs) -> Ingestion:
    self.type = IngestionSinkType.BIGQUERY
    return BigQueryIngestion(fs=fs)
  
  def build_redshift(self, fs, connection_string: str) -> Ingestion:
    self.type = IngestionSinkType.REDSHIFT
    return RedshiftIngestion(fs=fs, connection_string=connection_string)