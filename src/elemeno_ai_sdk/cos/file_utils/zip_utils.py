
import os
import glob
from zipfile import ZipFile
from typing import List, Optional


class ZipUtils:
  """ Utils class to zip and unzip files """

  def __init__(self):
    pass

  def zip_folder(self, folder_path: str, dest_filename: str,
   excludes: Optional[List[str]] = None) -> str:
    """
    Zip a folder.
     - param folder_path: The path to the folder to zip.
     - param dest_filename: The path to the destination file.
     - param excludes: A list of files to exclude from the zip.
    Returns:
      The path to the zip file.
    """
    if not os.path.exists(folder_path):
      raise ValueError("The folder path doesn't exist")
    with ZipFile(dest_filename, "w") as zip_file:
      src = glob.glob(f'{folder_path}/**/*', recursive=True)
      if excludes is not None:
        src = [s for s in src if s not in excludes]
      _ = [zip_file.write(f) for f in src]
    return dest_filename

  def unzip_file(self, zip_file_path: str, dest_folder_path: Optional[str] = None) -> str:
    """
    Unzip a file.
     - param zip_file_path: The path to the zip file.
     - param dest_folder_path: The path to the destination folder.
    Returns:
      The path to the folder.
    """
    if dest_folder_path is not None and not os.path.exists(zip_file_path):
      raise ValueError("The zip file path doesn't exist")
    with ZipFile(zip_file_path, "r") as zip_file:
      zip_file.extractall(dest_folder_path)
    return dest_folder_path
