
import abc
from typing import Dict, List

import pandas as pd


class FileIngestion(abc.ABC):

  @abc.abstractmethod
  def io_batch_ingest(self, to_ingest: List[Dict], media_id_col: str, media_url_col: str, dest_folder_col: str):
    pass