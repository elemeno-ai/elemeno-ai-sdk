
import io
import os
import pandas as pd
from typing import Dict, List, Optional
from elemeno_ai_sdk.cos.minio import MinioClient
from elemeno_ai_sdk.config import logging
from multiprocessing import Pool, cpu_count
from functools import partial
from minio import S3Error
import requests
from elemeno_ai_sdk.config import Configs
from elemeno_ai_sdk.ml.features.ingest.sink.file_ingestion import FileIngestion, MediaColumn

class IngestionParams:

  def __init__(self, media_path_col: str, to_ingest: Dict, dest_folder: Optional[str], 
    dest_folder_col: Optional[str] = None, media_name_col: Optional[str] = None,
    minio_bucket: str = "elemeno-cos"):

    self.media_path_col = media_path_col
    self.dest_folder = dest_folder
    self.dest_folder_col = dest_folder_col
    self.media_name_col = media_name_col
    self.minio_bucket = minio_bucket
    self.to_ingest = to_ingest

class DigestionParams:

  def __init__(self, media_path_col: str, to_digest: Dict, remote_folder: Optional[str], 
    remote_folder_col: Optional[str] = None, media_name_col: Optional[str] = None,
    minio_bucket: str = "elemeno-cos"):

    self.media_path_col = media_path_col
    self.remote_folder = remote_folder
    self.remote_folder_col = remote_folder_col
    self.media_name_col = media_name_col
    self.minio_bucket = minio_bucket
    self.to_digest = to_digest

def minio_client():
  config = Configs.instance()
  client = MinioClient(host=config.cos.host,
    access_key=config.cos.key_id,
    secret_key=config.cos.secret,
    use_ssl=config.cos.use_ssl,)
  return client
class MinioIngestion(FileIngestion):

  def __init__(self):
    self.config = Configs.instance()
    pass

  def io_batch_ingest_from_df(self, feature_table_name: str, to_ingest: pd.DataFrame, media_columns: List[MediaColumn]):
    local_path_cols = filter(lambda x: x if "http" not in x.name and "https" not in x.name else None, media_columns)
    remote_path_cols = filter(lambda x: x if "http" in x.name or "https" in x.name else None, media_columns)
    df_dict = to_ingest.to_dict('records')
    for col in local_path_cols:
      if col and col.is_upload:
        logging.info("Uploading {} files from media column {}".format(len(df_dict), col.name))
        dest_folder_name = f"{feature_table_name}_{col.name}"
        pool = Pool(cpu_count())
        raw = map(lambda x: IngestionParams(media_path_col=col.name, to_ingest=x, dest_folder=dest_folder_name, minio_bucket=self.config.cos.bucket), df_dict)
        upload_func = partial(self.upload_file_to_remote)
        pool.map(upload_func, raw)
        pool.close()
        pool.join()
    
    for col in remote_path_cols:
      if col and not col.is_upload:
        logging.info("Downloading {} files from media column {} to the remote persistence".format(len(df_dict), col))
        pool = Pool(cpu_count())
        raw = map(lambda x: IngestionParams(media_path_col=col.name, to_ingest=x, dest_folder=dest_folder_name, minio_bucket=self.config.cos.bucket), df_dict)
        pool = Pool(cpu_count())
        download_func = partial(self.download_file_to_remote)
        pool.map(download_func, raw)
        pool.close()
        pool.join()
    logging.info("Finished processing binary files ingestion")
    return None

  def io_batch_ingest(self, to_ingest: List[Dict]):
    config = self.config
    print("prepare map")
    raw = map(lambda x: IngestionParams(config.feature_store.source.params.binary.media_id_col, config.feature_store.source.params.binary.media_url_col,
          config.feature_store.source.params.binary.dest_folder_col,
          to_ingest=x), to_ingest)
    pool = Pool(cpu_count())
    download_func = partial(self.download_file_to_remote)
    print("start map for downloading files")
    pool.map(download_func, raw)
    pool.close()
    pool.join()
    return None
  
  def _remove_duplicates(self, df_dict, media_columns):
    """
    Remove duplicated rows from a list of dictionaries based on the values of
    the specified media columns.
    
    Parameters:
    - df_dict: a list of dictionaries representing a DataFrame
    - media_columns: a list of column names to use as the criteria for detecting duplicates
    
    Returns:
    A list of dictionaries with duplicated rows removed based on the specified media columns.
    """
    unique_rows = []
    seen_media = set()
    for row in df_dict:
        media = tuple(row[col.name] for col in media_columns)
        if media not in seen_media:
            unique_rows.append(row)
            seen_media.add(media)
    return unique_rows
  
  def io_batch_digest(self, feature_table_name: str, to_digest: pd.DataFrame, media_columns: List['MediaColumn']):
    # guarantee unique values on medias
    df_dict = to_digest.to_dict('records')
    # remove duplicated records from the dict
    self._remove_duplicates(df_dict, media_columns)
    for col in media_columns:
      logging.info("Downloading {} files from media column {}".format(len(to_digest), col.name))
      remote_folder_name = f"{feature_table_name}_{col.name}"
      pool = Pool(cpu_count())
      raw = map(lambda x: DigestionParams(media_path_col=col.name, to_digest=x, remote_folder=remote_folder_name, minio_bucket=self.config.cos.bucket), df_dict)
      download_func = partial(self.download_file_from_remote)
      pool.map(download_func, raw)
      pool.close()
      pool.join()
    logging.info("Finished processing binary files digestion")
    return None


  def download_file_from_remote(self, p: 'DigestionParams'):
    client = minio_client()
    logging.info("Processing {}".format(type(p)))
    
    to_digest = p.to_digest
    file_path = to_digest[p.media_path_col]
    file_path = os.path.normpath(file_path)
    folder_remote = to_digest[p.remote_folder_col] if p.remote_folder_col else p.remote_folder
    # remove ./ from the file name when it's present
    folder_remote = os.path.normpath(folder_remote)
    bucket = p.minio_bucket

    # check if bucket exists on minio
    if not client.bucket_exists(bucket):
      raise ValueError("The specified bucket does not exists")

    try:
      local_folder = ".binaries"
      if not os.path.exists(local_folder):
        os.makedirs(local_folder)
      local_file_path = os.path.join(local_folder, folder_remote, file_path)

      client.get_object(bucket, f"{folder_remote}/{file_path}", local_file_path)
      logging.debug("Downloaded file {} from bucket {} and folder {}".format(file_path, bucket, folder_remote))
    except (Exception, S3Error) as e:
      logging.error("Error downloading file {} from bucket {} and folder {}. Error: {}".format(file_path, bucket, folder_remote, e))
    return None

  def upload_file_to_remote(self, p: 'IngestionParams'):
    client = minio_client()
    logging.info("Processing {}".format(type(p)))

    to_ingest = p.to_ingest
    file_path = to_ingest[p.media_path_col]
    file_path = os.path.normpath(file_path)
    folder_remote = to_ingest[p.dest_folder_col] if p.dest_folder_col else p.dest_folder
    folder_remote = os.path.normpath(folder_remote)
    bucket = p.minio_bucket
    
    try:
      with open(file_path, 'rb') as file_data:
        file_contents = file_data.read()
        client.put_object(bucket, f"{folder_remote}/{file_path}", io.BytesIO(file_contents))
        logging.debug("Uploaded file {} to bucket {} and folder {}".format(file_path, bucket, folder_remote))
    except Exception as e:
      logging.error(f"error uploading file {file_path} to folder: {folder_remote}")
      logging.error(e)
    pass

  def download_file_to_remote(self, p: 'IngestionParams'):
    try:
      logging.error("Processing {}".format(type(p)))
      
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
              self.client.put_object('elemeno-cos', f"binary_data_parallel/{folder_id}/{position}_{media_id}.{media_extension}", st)
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