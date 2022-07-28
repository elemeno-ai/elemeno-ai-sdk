import json
from typing import Dict, List, Optional
from elasticsearch import Elasticsearch
from typer import Option
from elemeno_ai_sdk import logger
import pandas as pd
from .base_source import BaseSource, ReadResponse

class ElasticIngestionSource(BaseSource):

  def __init__(self, host: str, username: str, password: str):
    super().__init__()
    self._es = Elasticsearch(hosts=[host],
            http_auth=(username, password))
  
  def read(self, index: str = "", query: str = "", max_per_page: int = 1000, max_pages: Optional[int] = None, 
    binary_columns: Optional[List[str]] = None, media_id_col: Optional[str] = None, dest_folder_col: Optional[str] = None) -> ReadResponse:
    """ Reads data from elastic.

    args:
    
    - index: The index to read from.
    - query: The query to use.
    - max_per_page: The maximum number of records to read per page.
    - binary_columns: A list of columns containing links to binary files (jpeg, pdf, etc) that will be downloaded by a Sink.
    - media_id_col: The column containing the media id.
    - dest_folder_col: The column containing the destination folder.
  
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
        binary_prepared: List[Dict] = None
        if binary_columns is not None:
          binary_prepared = []
          for bin_col in binary_columns:
            binary_prepared.extend(self.prepare_medias(res['hits']['hits'], bin_col, media_id_col, dest_folder_col))
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
    return ReadResponse(dataframe=pd.DataFrame(all_results), prepared_medias=binary_prepared)

  def read_after(self, timestamp_str: str, index: str = "", query: str = "", max_per_page: int = 1000, 
    binary_columns: Optional[List[str]] = None, media_id_col: Optional[str] = None, dest_folder_col: Optional[str] = None) -> ReadResponse:
    """ Read data after a given timestamp. 
    When using this function you can't specify a range filter in the query.

    args:
    
    - timestamp_str: A string representing a timestamp. It should have the same format used in the source elastic.
    - index: The index to read from.
    - query: The query to use.
    - max_per_page: The maximum number of records to read per page.
    - binary_columns: A list of columns containing links to binary files (jpeg, pdf, etc) that will be downloaded by a Sink.
    - media_id_col: The column containing the media id.
    - dest_folder_col: The column containing the destination folder.
    
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
    return self.read(index, query, max_per_page, binary_columns, media_id_col, dest_folder_col)

  def prepare_medias(self, properties: List[Dict], binary_col: str, media_id_col: str, dest_folder_col: str) -> List:
    for p in properties:
      for i, m in enumerate(p['_source'][binary_col]):
        m['position'] = i
    # sort the properties by number of assets in descending order
    sorted_properties = sorted(properties, key=lambda x: len(x['_source'][binary_col]), reverse=True)

    # create a list of lists to hold the resulting array
    result = []

    # iterate over the properties
    for property in sorted_properties:
      for asset in property['_source'][binary_col]:
        asset[dest_folder_col] = property['_source'][media_id_col]
        result.append(asset)

    return result