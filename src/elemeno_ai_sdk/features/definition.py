import typing
import feast
from elemeno_ai_sdk.features.feast_elm  import Feast as FeastElemeno

class BigQueryDataSource:
    def __init__(self, project: str, dataset: str):
        self.project = project
        self.dataset = dataset

class FeatureTableDefinition:

    def __init__(self, name: str, event_column: str,
            batch_source: BigQueryDataSource, feast_elm: FeastElemeno,
            entities: typing.List[feast.Entity] = None,
            features: typing.List[feast.Feature] = None):
        self.name = name
        self.evt_col = event_column
        self.batch_source = batch_source
        self.entities: typing.List[feast.Entity] = [] if entities is None else entities
        self.features = [] if features is None else features
        self._feast_elm = feast_elm

    def register_entities(self, *entities: feast.Entity) -> None:
        self.entities.extend(list(entities))

    def register_features(self, *features: feast.Feature) -> None:
        self.features.extend(list(features))

    def register_entity(self, entity: feast.Entity) -> None:
        self.entities.append(entity)

    def register_feature(self, feature: feast.Feature) -> None:
        self.features.append(feature)

    def _get_ft(self, broker=None):
        bq = self.batch_source
        if broker is None:
            return feast.FeatureTable(
                name = self.name,
                entities = [e.name for e in self.entities],
                features = self.features,
                batch_source = feast.data_source.BigQuerySource(
                event_timestamp_column=self.evt_col,
                table_ref=bq.project + ":" + bq.dataset + "." + self.name,
                created_timestamp_column="created_timestamp",
                date_partition_column=self.evt_col
                )
            )
        return feast.FeatureTable(
            name = self.name,
            entities = [e.name for e in self.entities],
            features = self.features,
            batch_source = feast.data_source.BigQuerySource(
                event_timestamp_column=self.evt_col,
                table_ref=bq.project + ":" + bq.dataset + "." + self.name,
                created_timestamp_column="created_timestamp",
                date_partition_column=self.evt_col
            ), stream_source = KafkaSource(
                event_timestamp_column=self.evt_col,
                created_timestamp_column="created_timestamp",
                bootstrap_servers=broker,
                topic=self.name,
                message_format=AvroFormat(get_avro_schema(table, data))
            ))
    
    def apply(self) -> None:
        self._feast_elm.apply(self.entities)
        f = self._get_ft()
        self._feast_elm.apply(f)


