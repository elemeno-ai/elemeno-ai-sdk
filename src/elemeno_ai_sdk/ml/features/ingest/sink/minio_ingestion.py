
import io
from typing import Dict, List
from elemeno_ai_sdk.cos.minio import MinioClient
from elemeno_ai_sdk.config import logging
from multiprocessing import Pool, cpu_count
from functools import partial
import requests
from elemeno_ai_sdk.config import Configs
from elemeno_ai_sdk.ml.features.ingest.sink.file_ingestion import FileIngestion

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
class MinioIngestion(FileIngestion):

  def io_batch_ingest(self, to_ingest: List[Dict]):
    config = Configs.instance()
    print("prepare map")
    raw = map(lambda x: IngestionParams(config.cos.host, 
          config.cos.key_id, config.cos.secret, config.cos.use_ssl,
          config.feature_store.source.params.binary.media_id_col, config.feature_store.source.params.binary.media_url_col,
          config.feature_store.source.params.binary.dest_folder_col,
          to_ingest=x), to_ingest)
    pool = Pool(cpu_count())
    download_func = partial(self.download_file_to_remote, raw)
    print("start map for downloading files")
    pool.map(download_func)
    pool.close()
    pool.join()
    return None

  def download_file_to_remote(self, p: 'IngestionParams'):
    try:
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
    except Exception as e:
      logging.error("Error downloading file, will ignore")
      logging.error(e)