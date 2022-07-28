
import enum
from elemeno_ai_sdk.ml.features.ingest.sink.base_ingestion import Ingestion

from elemeno_ai_sdk.ml.features.ingest.sink.bigquery_ingestion import BigQueryIngestion
from elemeno_ai_sdk.ml.features.ingest.sink.redshift_ingestion import RedshiftIngestion
from elemeno_ai_sdk.ml.features.ingest.sink.redshift_test_ingestion import RedshiftTestIngestion

class IngestionSinkType(str, enum.Enum):
  BIGQUERY = "BigQuery"
  REDSHIFT = "Redshift"
  _REDSHIFT_UNIT_TESTS = "RedshiftUnitTests"

class FileIngestionSinkType(str, enum.Enum):
  MINIO = "Minio"
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
  
  def _build_redshift_unit_tests(self, fs, connection_string: str) -> RedshiftTestIngestion:
    """
    Builds a Redshift sink
    Internal method, should only be used in unit tests

    args:
    
    - fs: FeatureStore instance
    - connection_string: Connection string to Redshift database. Use SQLAlchemy syntax here.

    return:
    
    - Redshift Ingestion instance
    """
    self.type = IngestionSinkType._REDSHIFT_UNIT_TESTS
    return RedshiftTestIngestion(fs=fs, connection_string=connection_string)