
import io
import os
from typing import Dict, List, Optional, Tuple
from dask.distributed import Client
import requests
from elemeno_ai_sdk.config import Configs
from elemeno_ai_sdk.ml.features.ingest.sink.file_ingestion import FileIngestion


def install():
  import os
  import sys
  os.system("pip install minio")

def io_batch_dask(params: Tuple['IngestionParams', Dict]):
  from elemeno_ai_sdk.cos.minio import MinioClient
  from elemeno_ai_sdk.config import logging

  config, to_ingest = params
  client = MinioClient(host=config.minio_host,
    access_key=config.minio_user,
    secret_key=config.minio_pass,
    use_ssl=config.minio_ssl)
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

class IngestionParams:

  def __init__(self, minio_host: str, minio_user: str, minio_pass: str, minio_ssl: bool):
    self.minio_host = minio_host
    self.minio_user = minio_user
    self.minio_pass = minio_pass
    self.minio_ssl = minio_ssl
class MinioIngestionDask(FileIngestion):

  def __init__(self, dask_uri: Optional[str] = None):
    dask_client = Client(dask_uri, timeout=300)
    self.dask_client = dask_client
    dask_client.run(install)
    dask_client.upload_file(os.getenv('ELEMENO_CFG_FILE', 'elemeno.yaml'))
    dask_client.upload_file(os.getenv('FEAST_CONFIG_PATH', 'feature_store.yaml'))
  

  def io_batch_ingest(self, to_ingest: List[Dict], media_id_col: str, media_url_col: str, dest_folder_col: str):
    config = Configs.instance()
    params = []
    for d in to_ingest:
      d['media_id_col'] = media_id_col
      d['media_url_col'] = media_url_col
      d['dest_folder_col'] = dest_folder_col

      i = IngestionParams(config.cos.host, config.cos.key_id, config.cos.secret, config.cos.use_ssl)
      params.append((i, d))
    m = self.dask_client.map(io_batch_dask, params, batch_size=500)
    self.dask_client.compute(m)
    print("Finished dask processing result")