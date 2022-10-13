
from io import BytesIO, StringIO
import os
from typing import Optional
import pandas as pd
from google.cloud.storage import Client
from elemeno_ai_sdk.ml.features.ingest.source.base_source import BaseSource
from elemeno_ai_sdk import logger

class GCSIngestionSource(BaseSource):

  def __init__(self, bucket_name: str, folder_prefix: Optional[str]=None):
    """ Initializes a GCS ingestion source.

    args:

    - bucket_name: The name of the bucket.
    - folder_prefix: The folder prefix to be used to query the GCS.

    """
    self.bucket_name = bucket_name
    self.folder_prefix = folder_prefix

  def read(self, **kwargs) -> pd.DataFrame:
    """ Reads data from the GCS source.

    args:

    - folder_prefix: The folder prefix to be used to query the GCS.

    return:

    - A dataframe containing the data.
    """
    c = Client()
    bucket = c.bucket(self.bucket_name)
    blobs = bucket.list_blobs(prefix=self.folder_prefix)
    for blob in blobs:
      #blob.download_to_filename(os.path.join(".elemeno_stage", blob.name))
      contents = blob.download_as_string()
      logger.info("Downloaded file: {}".format(blob.name))
      df = pd.read_csv(BytesIO(contents), delimiter=";", on_bad_lines='skip')
      yield df
  
  def read_after(self, timestamp_str: str, **kwargs) -> pd.DataFrame:
    raise NotImplementedError("GCSIngest does not support read_after")
