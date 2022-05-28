import json
from elasticsearch import Elasticsearch
from elemeno_ai_sdk import logger
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
      res = self._es.search(index=index, query=query, size=count)
      if not 'hits' in res or not 'hits' in res['hits']:
        raise Exception("No hits found")
      sources = [hit['_source'] for hit in res['hits']['hits']]
      return pd.DataFrame(sources)
    all_results = []
    search_after = 0
    pages = count // max_per_page + 1
    for page in range(0, pages):
      logger.info("Reading page %d of %d", page, pages)
      logger.info("Size of page: %d", max_per_page)
      res = self._es.search(index=index, query=query, size=max_per_page, sort=[{"updated_date": "asc"}], search_after=[search_after])
      if 'hits' in res and 'hits' in res['hits']:
        sources = [hit['_source'] for hit in res['hits']['hits']]
        all_hits = res['hits']['hits']
        if len(all_hits) > 0 and 'sort' in all_hits[-1]:
          sort_response = search_after = all_hits[-1]['sort']
          all_results.extend(sources)
          if len(sort_response) == 0:
            logger.info("Exhausted pages")
            break
          search_after = sort_response[0]
    return pd.DataFrame(all_results)