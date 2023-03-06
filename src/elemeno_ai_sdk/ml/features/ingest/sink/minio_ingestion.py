
import io
import pandas as pd
from typing import Dict, List, Optional
from elemeno_ai_sdk.cos.minio import MinioClient
from elemeno_ai_sdk.config import logging
from multiprocessing import Pool, cpu_count
from functools import partial
import requests
from elemeno_ai_sdk.config import Configs
from elemeno_ai_sdk.ml.features.ingest.sink.file_ingestion import FileIngestion, MediaColumn

class IngestionParams:

  def __init__(self, minio_host: str, minio_user: str, minio_pass: str, minio_ssl: bool, 
    media_path_col: str, to_ingest: Dict, dest_folder: Optional[str], 
    dest_folder_col: Optional[str] = None, media_name_col: Optional[str] = None,
    minio_bucket: str = "elemeno-cos"):

    self.minio_host = minio_host
    self.minio_user = minio_user
    self.minio_pass = minio_pass
    self.minio_ssl = minio_ssl
    self.media_path_col = media_path_col
    self.dest_folder = dest_folder
    self.dest_folder_col = dest_folder_col
    self.media_name_col = media_name_col
    self.minio_bucket = minio_bucket
    self.to_ingest = to_ingest
class MinioIngestion(FileIngestion):

  def io_batch_ingest_from_df(self, feature_table_name: str, to_ingest: pd.DataFrame, media_columns: List[MediaColumn]):
    config = Configs.instance()
    local_path_cols = filter(lambda x: x if "http" not in x.name and "https" not in x.name else None, media_columns)
    remote_path_cols = filter(lambda x: x if "http" in x.name or "https" in x.name else None, media_columns)
    df_dict = to_ingest.to_dict('records')
    for col in local_path_cols:
      if col and col.is_upload:
        logging.info("Uploading {} files from media column {}".format(len(df_dict), col.name))
        dest_folder_name = f"{feature_table_name}_{col.name}"
        pool = Pool(cpu_count())
        raw = map(lambda x: IngestionParams(config.cos.host, config.cos.key_id, config.cos.secret, config.cos.use_ssl,
          media_path_col=col.name, to_ingest=x, dest_folder=dest_folder_name, minio_bucket=config.cos.bucket), df_dict)
        upload_func = partial(self.upload_file_to_remote)
        pool.map(upload_func, raw)
        pool.close()
        pool.join()
    
    for col in remote_path_cols:
      if col and not col.is_upload:
        logging.info("Downloading {} files from media column {} to the remote persistence".format(len(df_dict), col))
        pool = Pool(cpu_count())
        raw = map(lambda x: IngestionParams(config.cos.host, config.cos.key_id, config.cos.secret, config.cos.use_ssl,
          media_path_col=col.name, to_ingest=x, dest_folder=dest_folder_name, minio_bucket=config.cos.bucket), df_dict)
        pool = Pool(cpu_count())
        download_func = partial(self.download_file_to_remote)
        pool.map(download_func, raw)
        pool.close()
        pool.join()
    logging.info("Finished processing binary files ingestion")
    return None

  def io_batch_ingest(self, to_ingest: List[Dict]):
    config = Configs.instance()
    print("prepare map")
    raw = map(lambda x: IngestionParams(config.cos.host, 
          config.cos.key_id, config.cos.secret, config.cos.use_ssl,
          config.feature_store.source.params.binary.media_id_col, config.feature_store.source.params.binary.media_url_col,
          config.feature_store.source.params.binary.dest_folder_col,
          to_ingest=x), to_ingest)
    pool = Pool(cpu_count())
    download_func = partial(self.download_file_to_remote)
    print("start map for downloading files")
    pool.map(download_func, raw)
    pool.close()
    pool.join()
    return None

  def upload_file_to_remote(self, p: 'IngestionParams'):
    logging.info("Processing {}".format(type(p)))
    client = MinioClient(host=p.minio_host,
      access_key=p.minio_user,
      secret_key=p.minio_pass,
      use_ssl=p.minio_ssl)

    to_ingest = p.to_ingest
    file_path = to_ingest[p.media_path_col]
    folder_remote = to_ingest[p.dest_folder_col] if p.dest_folder_col else p.dest_folder
    bucket = p.minio_bucket
    
    try:
      with open(file_path, 'rb') as file_data:
        client.put_object(bucket, f"{folder_remote}/{file_path}", file_data)
        logging.debug("Uploaded file {} to bucket {} and folder {}".format(file_path, bucket, folder_remote))
    except Exception as e:
      logging.error(f"error uploading file {file_path} to folder: {folder_remote}")
      logging.error(e)
    pass

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