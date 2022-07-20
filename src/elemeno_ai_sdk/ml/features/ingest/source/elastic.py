import json
from typing import Optional
from elasticsearch import Elasticsearch
from elemeno_ai_sdk import logger
import pandas as pd
from .base_source import BaseSource

class ElasticIngestionSource(BaseSource):

  def __init__(self, host: str, username: str, password: str):
    super().__init__()
    self._es = Elasticsearch(hosts=[host],
            http_auth=(username, password))
  
  def read(self, index: str = "", query: str = "", max_per_page: int = 1000, max_pages: Optional[int] = None) -> pd.DataFrame:
    """ Reads data from elastic.

    args:
    
    - index: The index to read from.
    - query: The query to use.
    - max_per_page: The maximum number of records to read per page.
  
    return:
    
    - A pandas dataframe.
    """
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
      if max_pages is not None and page >= max_pages:
        break
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
        else:
          all_results.extend(sources)
    return pd.DataFrame(all_results)

  def read_after(self, timestamp_str: str, index: str = "", query: str = "", max_per_page: int = 1000) -> pd.DataFrame:
    """ Read data after a given timestamp. 
    When using this function you can't specify a range filter in the query.

    args:
    
    - timestamp_str: A string representing a timestamp. It should have the same format used in the source elastic.
    - index: The index to read from.
    - query: The query to use.
    - max_per_page: The maximum number of records to read per page.
    
    return:
    
    - A pandas dataframe with the result.
    """

    if "query" not in query:
      query["query"] = {}
    if "range" in query["query"]:
      raise ValueError("Cannot specify range in query and use read_after, use read instead")
    query["query"]["range"] = {"event_timestamp": {
      "gt": timestamp_str}
    } 
    return self.read(index, query, max_per_page)