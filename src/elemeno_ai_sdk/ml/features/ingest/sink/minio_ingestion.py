
import io
from typing import Dict, List, Optional
from dask.distributed import Client
from distributed import as_completed
import requests
from elemeno_ai_sdk.ml.features.ingest.sink.file_ingestion import FileIngestion
from elemeno_ai_sdk.config import logging

class MinioIngestionDask(FileIngestion):

  def __init__(self, dask_uri: Optional[str] = None):
    self.dask_client = Client(dask_uri, timeout=300)

  def _prepare_ingest(self):
    self.dask_client.run(install)
    self.dask_client.upload_file('elemeno.yaml')
    self.dask_client.upload_file('feature_store.yaml')

  def io_batch_ingest(self, to_ingest: List[Dict], media_id_col: str, media_url_col: str, dest_folder_col: str):
    for d in to_ingest:
      d['media_id_col'] = media_id_col
      d['media_url_col'] = media_url_col
      d['dest_folder_col'] = dest_folder_col
    self._prepare_ingest()
    m = self.dask_client.map(io_batch_dask, to_ingest, batch_size=500)
    self.dask_client.compute(m)
    print("Finished dask processing result")

def io_batch_dask(to_ingest: Dict):
  from elemeno_ai_sdk.cos.minio import MinioClient
  from elemeno_ai_sdk.config import logging, Configs
  
  config = Configs.instance()
  client = MinioClient(host=config.cos.host,
    access_key=config.cos.key_id,
    secret_key=config.cos.secret,
    use_ssl=config.cos.use_ssl)
  headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"}
  media_id = to_ingest[to_ingest['media_id_col']]
  media_url = to_ingest[to_ingest['media_url_col']].replace("\\", "")
  folder_id = to_ingest[to_ingest['dest_folder_col']]
  position = to_ingest['position']
  media_url = media_url.replace('/{description}.', "/x.")

  r = requests.get(media_url, headers=headers, stream=True)
  content_type = r.headers['content-type']
  if content_type.startswith('image'):
      st = io.BytesIO(r.content)
      media_extension = media_url.split('.')[-1]
      try:
          client.put_object('elemeno-cos', f"binary_data_parallel/{folder_id}/{position}_{media_id}.{media_extension}", st)
          logging.error("uploaded path: " + f"binary_data_parallel/{folder_id}/{position}_{media_id}.{media_extension}")
          return True
      except Exception as e:
          logging.error("error uploading file to folder: " + folder_id)
          logging.error(e)
          return False
  else:
    logging.error(f'{media_id}: {media_url}')
    logging.error("Not an image")
    return False

def install():
  import os
  import sys
  os.system(f"{sys.executable} -m pip install elemeno-ai-sdk==0.2.40 minio")