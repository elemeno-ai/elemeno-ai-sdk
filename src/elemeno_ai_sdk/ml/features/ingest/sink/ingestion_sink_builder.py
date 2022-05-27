
import enum
from elemeno_ai_sdk.ml.features.feature_store import BaseFeatureStore

from elemeno_ai_sdk.ml.features.ingest.sink.bigquery_ingestion import BigQueryIngestion
from elemeno_ai_sdk.ml.features.ingest.sink.redshift_ingestion import RedshiftIngestion

class IngestionSinkType(enum.Enum):
  BIGQUERY = "BigQuery"
  REDSHIFT = "Redshift"

class IngestionSinkBuilder:

  def __init__(self):
    self.type = type
  
  def build_bigquery(self, fs: BaseFeatureStore) -> BigQueryIngestion:
    self.type = IngestionSinkType.BIGQUERY
    return BigQueryIngestion(fs=fs)
  
  def build_redshift(self, fs: BaseFeatureStore, connection_string: str) -> RedshiftIngestion:
    self.type = IngestionSinkType.REDSHIFT
    return RedshiftIngestion(fs=fs, connection_string=connection_string)