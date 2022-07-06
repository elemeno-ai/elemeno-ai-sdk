from typing import Optional
import pandas as pd

import redshift_connector

from elemeno_ai_sdk.ml.features.ingest.source.base_source import BaseSource

MAX_LINES_TO_IMPORT = 10000


# TODO Add sql metadata validation
class RedshiftIngestionSource(BaseSource):

    def __init__(self, database: str, cluster_name: Optional[str]=None, base_query: Optional[str]=None,
      host: Optional[str]=None, port: Optional[int]=None, user: Optional[str]=None, password: Optional[str]=None):
      """ Initializes a Redshift ingestion source.
      
      args:

      - database: The name of the database.
      - cluster_name: The name of the cluster. Used when IAM authentication.
      - base_query: The base query to be used to query the Redshift.
      - host: The host of the Redshift instance.
      - port: The port of the Redshift instance.
      - user: The username of the Redshift instance.
      - password: The password of the Redshift instance.

      """
      if cluster_name is not None and (host is not None or port is not None or user is not None or password is not None):
        raise ValueError("When specifying cluster name you cannot specify host, port, user or password")
      self.host = host
      self.port = port
      self.user = user
      self.password = password
      self.cluster_name = cluster_name
      self.base_query = base_query
      self.database = database

    def read(self, base_query: Optional[str]=None, **kwargs) -> pd.DataFrame:
      """ Reads data from the Redshift source.

      args:

      - base_query: The base query to be used to query the Redshift.

      return:

      - A dataframe containing the data.
      """
      if base_query is not None:
        self.base_query = base_query
      with redshift_connector.connect(
          iam=True,
          database=self.database,
          db_user='awsuser',
          password='',
          user='',
          cluster_identifier=self.cluster_name,
          profile='default'
      ) as conn:
        with conn.cursor() as cursor:
          conn.autocommit = True
          cursor.execute(" {} ".format(self.base_query))
          return cursor.fetch_dataframe()

    def read_after(self, timestamp_str: str, base_query: Optional[str] = None, **kwargs) -> pd.DataFrame:
      """ Reads data from the Redshift source after a certain timestamp.

      args:

      - timestamp_str: The timestamp after which to read data.
      - base_query: The base query to be used to query the Redshift.

      return:

      - A dataframe containing the data.
      """
      if base_query is not None:
        self.base_query = base_query
      self.base_query = " {} WHERE event_timestamp > '{}' ORDER BY event_timestamp ASC LIMIT {}; " \
          .format(self.base_query, timestamp_str, MAX_LINES_TO_IMPORT)
      return self.read(base_query)
