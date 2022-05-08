import typing
import feast
import json
import logging
from google.protobuf.duration_pb2 import Duration
import pandas as pd
from sqlalchemy import create_engine
import pandas_gbq
from elemeno_ai_sdk.features.feature_store  import FeatureStore
from elemeno_ai_sdk.features.types import FeatureType

class FeatureTableDefinition:

    def __init__(self, name: str, feature_store: FeatureStore,
            entities: typing.List[feast.Entity] = None,
            features: typing.List[feast.Feature] = None,
            ttl_duration_weeks=52,
            online=False, event_column: str = "event_timestamp",
            created_column: str = "created_timestamp"):
        self.name = name
        self._entities = [] if entities is None else entities
        self._features = [] if features is None else features
        self._feast_elm = feature_store
        self._duration = ttl_duration_weeks
        self._online = online
        self._evt_col = event_column
        self._created_col = created_column
        self._table_schema = None


    @property
    def entities(self):
        return self._entities

    @property
    def evt_col(self):
        return self._evt_col

    @property
    def created_col(self):
        return self._created_col

    @property
    def table_schema(self):
        return self._table_schema

    @entities.setter
    def entities(self, value):
        self._entities = value

    def ingest_schema(self, schema_file_path: str) -> None:
        """
        This method should be called if you want to use a jsonschema file to create the feature table
        If other entities/features were registered, this method will append the ones in the jsonschema to them

        Arguments:
        schema_file_path: str - The local path to the file containing the jsonschema definition

        """
        with open(schema_file_path, mode="r") as schema_file:
            jschema = json.loads(schema_file.read())
            table_schema = []
            pd_schema = {}
            for name, prop in jschema["properties"].items():
                fmt = prop["format"] if "format" in prop else None
                table_schema.append({"name": name, "type": FeatureType.from_str_to_bq_type(prop["type"], format=fmt).name})
                pd_schema[name] = pd.Series(dtype=FeatureType.from_str_to_pd_type(prop["type"], format=fmt))
                if "isKey" in prop and prop["isKey"] == "true":
                    self.register_entity(feast.Entity(name=name, description=name, value_type=FeatureType.from_str_to_feature_type(prop["type"])))
                else:
                    if "format" in prop and prop["format"] == "date-time":
                        continue
                    self.register_features(feast.Feature(name, FeatureType.from_str_to_feature_type(prop["type"])))

            if len(list(filter(lambda x: x["name"] == self.created_col, table_schema))) == 0:
                table_schema.append({"name": self.created_col, "type": FeatureType.from_str_to_bq_type("string", format="date-time").name})
                pd_schema[self.created_col] = pd.Series(dtype=FeatureType.from_str_to_pd_type("string", format="date-time"))
            if len(list(filter(lambda x: x["name"] == self.evt_col, table_schema))) == 0:
                table_schema.append({"name": self.evt_col, "type": FeatureType.from_str_to_bq_type("string", format="date-time").name})
                pd_schema[self.evt_col] = pd.Series(dtype=FeatureType.from_str_to_pd_type("string", format="date-time"))

            logging.debug(f"FT bq types schema: {table_schema}")
            self._table_schema = table_schema
            logging.debug(f"Pandas types schema: {pd_schema}")
            df = pd.DataFrame(pd_schema)
            project_id = self._feast_elm.config.offline_store.project_id
            dataset = self._feast_elm.config.offline_store.dataset
            location = self._feast_elm.config.offline_store.location
            df.to_gbq(destination_table=f"{dataset}.{self.name}",
                project_id=project_id, if_exists="append", location=location)

    def ingest_schema_rs(self, schema_file_path: str, conn_str: str) -> None:
      """
      This method should be called if you want to use a jsonschema file to create the feature table
      If other entities/features were registered, this method will append the ones in the jsonschema to them

      Arguments:
      schema_file_path: str - The local path to the file containing the jsonschema definition

      """
      conn = create_engine(conn_str, isolation_level="AUTOCOMMIT")
      try:
        with open(schema_file_path, mode="r") as schema_file:
          jschema = json.loads(schema_file.read())
          table_schema = []
          pd_schema = {}
          for name, prop in jschema["properties"].items():
            fmt = prop["format"] if "format" in prop else None
            table_schema.append({"name": name, "type": FeatureType.from_str_to_bq_type(prop["type"], format=fmt).name})
            pd_schema[name] = pd.Series(dtype=FeatureType.from_str_to_pd_type(prop["type"], format=fmt))
            if "isKey" in prop and prop["isKey"] == "true":
              self.register_entity(feast.Entity(name=name, description=name, value_type=FeatureType.from_str_to_feature_type(prop["type"])))
            else:
              if "format" in prop and prop["format"] == "date-time":
                continue
              self.register_features(feast.Feature(name, FeatureType.from_str_to_feature_type(prop["type"])))

          if len(list(filter(lambda x: x["name"] == self.created_col, table_schema))) == 0:
            table_schema.append({"name": self.created_col, "type": FeatureType.from_str_to_bq_type("string", format="date-time").name})
            pd_schema[self.created_col] = pd.Series(dtype=FeatureType.from_str_to_pd_type("string", format="date-time"))
          if len(list(filter(lambda x: x["name"] == self.evt_col, table_schema))) == 0:
            table_schema.append({"name": self.evt_col, "type": FeatureType.from_str_to_bq_type("string", format="date-time").name})
            pd_schema[self.evt_col] = pd.Series(dtype=FeatureType.from_str_to_pd_type("string", format="date-time"))

          logging.info(f"FT bq types schema: {table_schema}")
          self._table_schema = table_schema
          logging.info(f"Pandas types schema: {pd_schema}")
          df = pd.DataFrame(pd_schema)
          # project_id = self._feast_elm.config.offline_store.project_id
          # dataset = self._feast_elm.config.offline_store.dataset
          # location = self._feast_elm.config.offline_store.location
          df.to_sql(f"{self.name}",
                    conn, index=False, if_exists='append')
      finally:
        conn.dispose()

    @property
    def features(self):
        return self._features

    @features.setter
    def features(self, value):
        self._features = value

    def register_entities(self, *entities: feast.Entity) -> None:
        self.entities.extend(list(entities))

    def register_features(self, *features: feast.Feature) -> None:
        self.features.extend(list(features))

    def register_entity(self, entity: feast.Entity) -> None:
        self.entities.append(entity)

    def register_feature(self, feature: feast.Feature) -> None:
        self.features.append(feature)

    def ingest(self, dataframe: pd.DataFrame):
        self._feast_elm.ingest(self._get_ft(), dataframe)

    def ingest_from_query(self, query: str):
      self._feast_elm.ingest_from_query(self._get_ft(), query)

    def ingest_rs(self, dataframe: pd.DataFrame, conn_str: str):
      expected_columns = self.all_columns()
      self._feast_elm.ingest_rs(self._get_ft_rs(), dataframe, conn_str, expected_columns, self._created_col)
    
    def all_columns(self) -> typing.List[str]:
        cols = []
        for e in self._entities:
            cols.append(e.name)
        for f in self._features:
            cols.append(f.name)
        cols.append(self._evt_col)
        cols.append(self._created_col)
        return cols

    def _get_ft(self):
        dataset = self._feast_elm.config.offline_store.dataset
        ft_source = feast.BigQuerySource(
            table_ref=f"{dataset}.{self.name}",
            event_timestamp_column=self.evt_col,
            created_timestamp_column=self.created_col
        )

        fv = feast.FeatureView(
            name = self.name,
            entities=[e.name for e in self._entities],
            ttl=Duration(seconds=self._duration * 604800),
            features=self._features,
            online=self._online,
            batch_source=ft_source,
            tags={}
        )
        self._feast_elm.apply(objects=self.entities)
        self._feast_elm.apply(objects=fv)

        return fv

    def _get_ft_rs(self):
      ft_source = feast.RedshiftSource(
        table= f"{self.name}",
        event_timestamp_column=self.evt_col,
        created_timestamp_column=self.created_col
      )

      fv = feast.FeatureView(
        name = self.name,
        entities=[e.name for e in self._entities],
        ttl=Duration(seconds=self._duration * 604800),
        features=self._features,
        online=self._online,
        batch_source=ft_source,
        tags={}
      )
      self._feast_elm.apply(objects=self.entities)
      self._feast_elm.apply(objects=fv)

      return fv

    def _get_ft_file(self):
      ft_source = feast.FileSource(
        path=f"data/{self.name}",
        event_timestamp_column=self.evt_col,
        created_timestamp_column=self.created_col
      )

      fv = feast.FeatureView(
        name = self.name,
        entities=[e.name for e in self._entities],
        ttl=Duration(seconds=self._duration * 604800),
        features=self._features,
        online=self._online,
        batch_source=ft_source,
        tags={}
      )
      self._feast_elm.apply(objects=self.entities)
      self._feast_elm.apply(objects=fv)

      return fv

    def get_view(self) -> feast.FeatureView:
        offline_store_type = type(self._feast_elm.fs.config.offline_store).__name__
        if (offline_store_type == "BigQueryOfflineStoreConfig"):
            f = self._get_ft()
        elif (offline_store_type == "RedshiftOfflineStoreConfig"):
            f = self._get_ft_rs()
        elif (offline_store_type == "FileOfflineStoreConfig"):
            f = self._get_ft_file()
        else:
            print(offline_store_type)
            raise ValueError("Unsupported offline store type")
        return f

