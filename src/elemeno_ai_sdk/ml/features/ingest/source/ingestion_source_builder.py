
import enum
from typing import Optional
from elemeno_ai_sdk.config import Configs

from elemeno_ai_sdk.ml.features.ingest.source.elastic import ElasticIngestionSource
from elemeno_ai_sdk.ml.features.ingest.source.bigquery import BigQueryIngestionSource
from elemeno_ai_sdk.ml.features.ingest.source.redshift import RedshiftIngestionSource
from elemeno_ai_sdk.ml.features.ingest.source.gcs import GCSIngestionSource

class IngestionSourceType(str, enum.Enum):
  ELASTIC = "Elastic"
  BIGQUERY = "BigQuery"
  REDSHIFT = "Redshift"
  GCS = "GCS"

class IngestionSourceBuilder:

  def __init__(self):
    """ Builder to create instancces of different types of Source"""
    self._config = Configs.instance()
    self.type = ""

  def build_gcs(self, bucket: Optional[str] = None, folder_prefix: Optional[str]=None) -> GCSIngestionSource:
    """ Builds a GCS ingestion source instance.
    
    args:
    
    - bucket: The name of the GCS bucket.
    - base_query: The base query to be used to query the GCS.
    
    return:
    
    - A GCS ingestion source instance.
    """
    if bucket is None:
      bucket = self._config.feature_store.source.params.bucket
    if folder_prefix is None:
      folder_prefix = self._config.feature_store.source.params.path
    self.type = IngestionSourceType.GCS
    return GCSIngestionSource(bucket_name=bucket, folder_prefix=folder_prefix)

  def build_elastic(self, host: Optional[str]=None, username: Optional[str]=None, password: Optional[str]=None) -> ElasticIngestionSource:
    """ Builds an Elastic ingestion source instance.

    args:
    
    - host: The host of the Elastic instance.
    - username: The username of the Elastic instance.
    - password: The password of the Elastic instance.
    
    return:
    
    - An Elastic ingestion source instance.
    """
    if host is None:
      host = self._config.feature_store.source.params.host
    if username is None:
      username = self._config.feature_store.source.params.username
    if password is None:
      password = self._config.feature_store.source.params.password
    self.type = IngestionSourceType.ELASTIC
    return ElasticIngestionSource(host=host, username=username, password=password)
  
  def build_big_query(self, project_id: Optional[str]=None, base_query: Optional[str] = None) -> BigQueryIngestionSource:
    """ Builds a BigQuery ingestion source instance.
    
    args:
    
    - project_id: The GCP project id.
    - base_query: The base query to be used to query the Big Query.
    
    return:
    
    - A BigQuery ingestion source instance.
    """
    if project_id is None:
      project_id = self._config.feature_store.source.params.project_id
    self.type = IngestionSourceType.BIGQUERY
    return BigQueryIngestionSource(gcp_project_id=project_id, base_query=base_query)

  def build_redshift(self, cluster_name: Optional[str]=None, host: Optional[str]=None,
    port: Optional[int]=None, user: Optional[str]=None, password: Optional[str]=None, database: Optional[str]=None, base_query: Optional[str]=None) -> RedshiftIngestionSource:
    """ Builds a Redshift ingestion source instance.
    
    args:
    
    - cluster_name: The name of the Redshift cluster. When specified we will use IAM authentication.
    - host: The host of the Redshift instance. Not used when cluster_name is specified.
    - port: The port of the Redshift instance. Not used when cluster_name is specified.
    - user: The username of the Redshift instance. Not used when cluster_name is specified.
    - password: The password of the Redshift instance. Not used when cluster_name is specified.
    - database: The name of the Redshift database.
    - base_query: The base query to be used to query the Redshift.
    
    return:
    
    - A Redshift ingestion source instance.
    """
    if cluster_name is None:
      cluster_name = self._config.feature_store.source.params.cluster_name
    if database is None:
      database = self._config.feature_store.source.params.database
    if host is None:
      host = self._config.feature_store.source.params.host
    if port is None:
      port = self._config.feature_store.source.params.port
    if user is None:
      user = self._config.feature_store.source.params.user
    if password is None:
      password = self._config.feature_store.source.params.password
    self.type = IngestionSourceType.REDSHIFT
    return RedshiftIngestionSource(cluster_name=cluster_name, database=database, base_query=base_query)