
import asyncio
import itertools
import logging
from multiprocessing.connection import wait
from typing import List, Dict
import requests
from dask.distributed import Client
from distributed import as_completed

import io

from elemeno_ai_sdk.cos.file_utils.elastic_to_cos import ElasticToCos

def install():
    import os
    os.system("pip install elemeno-ai-sdk==0.1.8 minio")

def get_and_upload_dask(to_get: Dict) -> None:
    import os
    from minio import Minio
    logging.basicConfig(level=logging.INFO)
    client = Minio(
            "minio.minio:9000",
            access_key="elemeno",
            secret_key="minio123",
            secure=False
        )
    logging.error("Starting logic")
    logging.error(to_get)
    if not '_source' in to_get:
      logging.error("Failed to process, missing _source in dict")
      raise Exception("Failure when reading _source")
    offer_id = to_get['_source']['id']
    for idx, media in enumerate(to_get['_source']['media']):
      try:
          media_id = media['id']
          media_url = media['url'].replace("\\", "")
          position = idx
          headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"}
          # fixes for bel description case
          media_url = media_url.replace('/{description}.', "/x.")
          r = requests.get(media_url, headers=headers, stream=True)
          content_type = r.headers['content-type']
          if content_type.startswith('image'):
              st = io.BytesIO(r.content)
              media_extension = media_url.split('.')[-1]
              try:
                  existing = client.list_dir('elemeno-cos', f"binary_data_parallel/{offer_id}/")
                  if len(list(existing)) == 0:
                      raise ValueError("list_dir didn't throw exception but content was 0")
                  #print("Will skip offer_id: " + offer_id)
              except Exception as e:
                  logging.error("will upload offer_id: " + offer_id)
                  #print(e)
                  client.put_object('elemeno-cos', f"binary_data_parallel/{offer_id}/{position}_{media_id}.{media_extension}", st, length=st.getbuffer().nbytes)
                  logging.error(f'{media_id}: {media_url}')
      except Exception as e:
        logging.error(f"Failed reading file URL {media['url']}")
        logging.error(e, stack_info=True)
        raise e
    return

if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  dask_client = Client(address="a6304c020a60a460cb97247a70f88d6f-805997406.us-east-1.elb.amazonaws.com:8786")
  dask_client.run(install, wait=True)
  elastic = ElasticToCos(None)
  loop = asyncio.get_event_loop()
  task = loop.create_task(elastic.query_file_urls("listing-br-rj-rio.de.janeiro-copacabana"))
  pages = loop.run_until_complete(task)
  all_futures = []
  for page in pages:
    hits = page['hits']['hits']
    print("submiting new tasks")
    futures = dask_client.map(get_and_upload_dask, hits)
    all_futures.append(futures)
  all_futures = itertools.chain(*all_futures)
  print("Will wait for futures to return")
  for f in as_completed(all_futures):
      try:
        print("waiting")
        f.result()
        print("finished")
      except Exception as e:
        logging.error(f"Failed getting file URL")
        logging.error(e)
        pass