
import io
import numpy as np
import os
from typing import Dict, List, Optional, Tuple
import dask.array as da
from dask.distributed import Client
import requests
from elemeno_ai_sdk.config import Configs
from elemeno_ai_sdk.ml.features.ingest.sink.file_ingestion import FileIngestion


def install():
  import os
  import sys
  os.system("pip install minio")

def io_batch_dask(params: List['IngestionParams']):
  from elemeno_ai_sdk.cos.minio import MinioClient
  from elemeno_ai_sdk.config import logging
  
  logging.error("Started batch dask")
  for p in params:
    logging.error("Processing {}".format(type(p)))
    client = MinioClient(host=p.minio_host,
      access_key=p.minio_user,
      secret_key=p.minio_pass,
      use_ssl=p.minio_ssl)
    headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"}
    to_ingest = p.to_ingest
    media_id = to_ingest[p.media_id_col]
    media_url = to_ingest[p.media_url_col].replace("\\", "")
    folder_id = to_ingest[p.dest_folder_col]
    position = to_ingest['position']
    media_url = media_url.replace('/{description}.', "/x.")

    r = requests.get(media_url, headers=headers, stream=True)
    content_type = r.headers['content-type']
    logging.error("Will check image")
    if content_type.startswith('image'):
        logging.error("Image found")
        st = io.BytesIO(r.content)
        media_extension = media_url.split('.')[-1]
        try:
            client.put_object('elemeno-cos', f"binary_data_parallel/{folder_id}/{position}_{media_id}.{media_extension}", st)
            logging.error("uploaded path: " + f"binary_data_parallel/{folder_id}/{position}_{media_id}.{media_extension}")
        except Exception as e:
            logging.error("error uploading file to folder: " + folder_id)
            logging.error(e)
    else:
      logging.error(f'{media_id}: {media_url}')
      logging.error("Not an image")
  return True

class IngestionParams:

  def __init__(self, minio_host: str, minio_user: str, minio_pass: str, minio_ssl: bool, 
    media_id_col: str, media_url_col: str, dest_folder_col: str, to_ingest: Dict):
    self.minio_host = minio_host
    self.minio_user = minio_user
    self.minio_pass = minio_pass
    self.minio_ssl = minio_ssl
    self.media_id_col = media_id_col
    self.media_url_col = media_url_col
    self.dest_folder_col = dest_folder_col
    self.to_ingest = to_ingest
class MinioIngestionDask(FileIngestion):

  def __init__(self, dask_uri: Optional[str] = None):
    dask_client = Client(dask_uri, timeout=300)
    self.dask_client = dask_client
    dask_client.run(install)
    dask_client.upload_file(os.getenv('ELEMENO_CFG_FILE', 'elemeno.yaml'))
    dask_client.upload_file(os.path.join(os.getenv('FEAST_CONFIG_PATH', '.'), 'feature_store.yaml'))
  

  def io_batch_ingest(self, to_ingest: List[Dict]):
    config = Configs.instance()
    mini_batches = []
    futures = []
    print("prepare map")
    errors = 0
    within_batch = 0
    batch = []
    for d in to_ingest:
      if within_batch == 0 or within_batch >= 20:
        mini_batches.append(batch)
        batch = []
        within_batch = 0
      try:
        i = IngestionParams(config.cos.host, 
          config.cos.key_id, config.cos.secret, config.cos.use_ssl,
          config.feature_store.source.params.binary.media_id_col, config.feature_store.source.params.binary.media_url_col,
          config.feature_store.source.params.binary.dest_folder_col,
          to_ingest=d)
        batch.append(i)
        print("added one more")
        print(i)
        within_batch += 1
      except Exception as e:
        print(e)
        errors += 1
        print("error submiting to dask ^")
        continue
    futures.extend(self.dask_client.map(io_batch_dask, mini_batches, pure=False))
    print("Num of submission errors: " + str(errors))
    print("Client will map to scheduler")
    self.dask_client.gather(futures, errors="skip")
    return None