
import enum
from elemeno_ai_sdk.ml.features.ingest.sink.base_ingestion import Ingestion

from elemeno_ai_sdk.ml.features.ingest.sink.bigquery_ingestion import BigQueryIngestion
from elemeno_ai_sdk.ml.features.ingest.sink.redshift_ingestion import RedshiftIngestion

class IngestionSinkType(enum.Enum):
  BIGQUERY = "BigQuery"
  REDSHIFT = "Redshift"

class IngestionSinkBuilder:

  def __init__(self):
    """ Builder to create instancces of different types of Sink"""
    self.type = ""
  
  def build_bigquery(self, fs) -> BigQueryIngestion:
    """"
    Builds a BigQuery sink
    
    args:
    - fs: FeatureStore instance

    return:
    - BigQuery Ingestion instance
    """
    self.type = IngestionSinkType.BIGQUERY
    return BigQueryIngestion(fs=fs)
  
  def build_redshift(self, fs, connection_string: str) -> RedshiftIngestion:
    """
    Builds a Redshift sink

    args:
    
    - fs: FeatureStore instance
    - connection_string: Connection string to Redshift database. Use SQLAlchemy syntax here.

    return:
    
    - Redshift Ingestion instance
    """
    self.type = IngestionSinkType.REDSHIFT
    return RedshiftIngestion(fs=fs, connection_string=connection_string)