from elasticsearch import Elasticsearch
import pandas as pd
from .base_source import BaseSource

class ElasticIngestion(BaseSource):

  def __init__(self, host: str, username: str, password: str):
    super().__init__()
    self._es = Elasticsearch(hosts=[host],
            http_auth=(username, password))
  
  def read(self, index: str, query: str, max_per_page: int = 1000) -> pd.DataFrame:
    count = self._es.count(index=index, query=query)["count"]
    if count <= max_per_page:
      res = self._es.search(index=index, query=query)
      if not 'hits' in res or not 'hits' in res['hits']:
        raise Exception("No hits found")
      return pd.DataFrame(res['hits']['hits'])
    all_results = []
    pages = count // max_per_page + 1
    for page in range(1, pages):
      res = self._es.search(index=index, query=query, size=max_per_page, from_=page*max_per_page)
      if 'hits' in res and 'hits' in res['hits']:
        all_results.extend(res['hits']['hits'])
    return pd.DataFrame(all_results)