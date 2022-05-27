
import enum

from elemeno_ai_sdk.ml.features.ingest.source.elastic import ElasticIngestion


class IngestionSourceType(enum.Enum):
  ELASTIC = "Elastic"

class IngestionSourceBuilder:

  def __init__(self):
    pass

  def build_elastic(self, host: str, username: str, password: str) -> ElasticIngestion:
    self.type = IngestionSourceType.ELASTIC
    return ElasticIngestion(host=host, username=username, password=password)