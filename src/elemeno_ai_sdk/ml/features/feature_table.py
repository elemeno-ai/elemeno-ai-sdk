import datetime
from typing import Any, Dict, List

import feast
import feast.types
from feast.data_format import JsonFormat

from elemeno_ai_sdk.ml.features.types import FeatureType


class FeatureTable:
    """A FeatureTable is the object that is used to define feature tables on Elemeno feature store.

    If you're looking to create a new feature table or read data look at ingest_schema of the class FeatureStore.
    """

    def __init__(
        self,
        name: str,
        feature_store: feast.FeatureStore = None,
        entities: List[feast.Entity] = None,
        features: List[feast.Feature] = None,
        ttl_duration_weeks=52,
        is_streaming=False,
        online=False,
        event_column: str = "event_timestamp",
        created_column: str = "created_timestamp",
    ):
        self.name = name
        self._entities = [] if entities is None else entities
        self._features = [] if features is None else features
        self._feast_elm = feature_store
        self._duration = ttl_duration_weeks
        self._online = online
        self.is_streaming = is_streaming
        self._evt_col = event_column
        self._created_col = created_column
        self._original_schema = None
        self._table_schema = []

    @property
    def entities(self) -> List[feast.Entity]:
        return self._entities

    @property
    def evt_col(self):
        return self._evt_col

    @property
    def created_col(self):
        return self._created_col

    @property
    def table_schema(self) -> List[Dict]:
        return self._table_schema

    @property
    def original_schema(self) -> Dict[str, Any]:
        return self._original_schema

    @entities.setter
    def entities(self, value):
        self._entities = value

    @property
    def features(self):
        return self._features

    @features.setter
    def features(self, value):
        self._features = value

    def set_table_schema(self, value: List[Dict]) -> None:
        self._table_schema = value

    def set_original_schema(self, value: List[Dict]) -> None:
        self._original_schema = value

    def register_entities(self, *entities: feast.Entity) -> None:
        self.entities.extend(list(entities))

    def register_features(self, *features: feast.Field) -> None:
        self.features.extend(list(features))

    def register_entity(self, entity: feast.Entity) -> None:
        self.entities.append(entity)

    def register_feature(self, feature: feast.Field) -> None:
        self.features.append(feature)

    def all_columns(self) -> List[str]:
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
        to_apply = []
        ft_source = feast.BigQuerySource(
            table_ref=f"{dataset}.{self.name}",
            event_timestamp_column=self.evt_col,
            created_timestamp_column=self.created_col,
        )
        fv = feast.FeatureView(
            name=self.name + "_stream", entities=self._entities, online=self._online, source=ft_source, tags={}
        )
        to_apply.append(fv)

        if self.is_streaming:
            ft_kafka_source = feast.KafkaSource(
                kafka_bootstrap_servers=self._elm_config.feature_store.stream_source.params.bootstrap_servers,
                topic=self._elm_config.feature_store.stream_source.params.topic,
                timestamp_field=self.evt_col,
                created_timestamp_column=self.created_col,
                message_format=JsonFormat(),
                batch_source=ft_source,
            )
            sfv = feast.StreamFeatureView(
                name=self.name,
                entities=self._entities,
                schema=self._features,
                online=self._online,
                source=ft_kafka_source,
            )
            to_apply.append(sfv)

        self._feast_elm.apply(objects=self.entities)
        self._feast_elm.apply(objects=to_apply)
        return fv

    def _get_ft_rs(self):
        to_apply = []
        ft_source = feast.RedshiftSource(
            name=self.name,
            table=f"{self.name}",
            created_timestamp_column=self.created_col,
            timestamp_field=self.evt_col,
        )
        fv = feast.FeatureView(name=self.name, entities=self._entities, online=self._online, source=ft_source, tags={})
        to_apply.append(fv)

        if self.is_streaming:
            ft_kafka_source = feast.KafkaSource(
                name=self.name + "_stream",
                kafka_bootstrap_servers=self._elm_config.feature_store.stream_source.params.bootstrap_servers,
                topic=self._elm_config.feature_store.stream_source.params.topic,
                timestamp_field=self.evt_col,
                created_timestamp_column=self.created_col,
                message_format=JsonFormat(
                    schema_json=", ".join(
                        [
                            str(field["name"] + " " + FeatureType.from_feast_to_json_schema_feast(field["type"]))
                            for field in self.table_schema
                        ]
                    )
                ),
                batch_source=ft_source,
                watermark_delay_threshold=datetime.timedelta(hours=72),
            )
            sfv = feast.StreamFeatureView(
                name=self.name + "_stream",
                entities=self._entities,
                schema=self._features,
                online=self._online,
                timestamp_field=self.evt_col,
                source=ft_kafka_source,
            )
            to_apply.append(sfv)
        self._feast_elm.apply(objects=self.entities)
        self._feast_elm.apply(objects=to_apply)

        return fv

    def _get_ft_file(self):
        ft_source = feast.FileSource(
            path=f"data/{self.name}", event_timestamp_column=self.evt_col, created_timestamp_column=self.created_col
        )

        fv = feast.FeatureView(
            name=self.name,
            entities=self._entities,
            schema=[feast.Field(name=f.name, dtype=feast.types.from_value_type(f.dtype)) for f in self._features],
            online=self._online,
            source=ft_source,
            tags={},
        )
        self._feast_elm.apply(objects=self.entities)
        self._feast_elm.apply(objects=fv)

        return fv

    def get_view(self) -> feast.FeatureView:
        offline_store_type = type(self._feast_elm.fs.config.offline_store).__name__
        if offline_store_type == "BigQueryOfflineStoreConfig":
            f = self._get_ft()
        elif offline_store_type == "RedshiftOfflineStoreConfig":
            f = self._get_ft_rs()
        elif offline_store_type == "FileOfflineStoreConfig":
            f = self._get_ft_file()
        else:
            print(offline_store_type)
            raise ValueError("Unsupported offline store type")
        return f
