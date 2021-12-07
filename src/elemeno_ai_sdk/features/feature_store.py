import typing
import logging
import feast
import pandas as pd

class FeatureStore: 
    def __init__(self, options:typing.Dict=None) -> None:
        o = {
            "CORE_URL": "feast-feast-core.feast:6565",
            "SERVING_URL":"feast-feast-online-serving.feast:6566",
            "JOB_SERVICE_URL": "feast-feast-jobservice.feast:6568",
            "SPARK_LAUNCHER": "standalone",
            "SPARK_HOME":"/opt/spark",
            "SPARK_STAGING_LOCATION":"/tmp/elabs-spark-staging",
            "HISTORICAL_FEATURE_OUTPUT_LOCATION":"gs://demo-spark-results",
            "GRPC_CONNECTION_TIMEOUT_DEFAULT": 5000,
            "BATCH_FEATURE_REQUEST_WAIT_S": "800"
        }
        if options is not None:
            o = o.update(options)
        self.options = o
        self.client = None

    @property
    def raw_client(self):
        self._check_connected()
        return self.client

    def _check_connected(self, throw=False) -> bool:
        is_connected = self.client is not None
        msg = "Client is not connected, call connect"
        if throw and not is_connected:
            raise ValueError(msg)
        if not is_connected:
            logging.warn("Client is not connected, it should call method connect first")
        return is_connected
    
    def connect(self) -> None:
        if self.client is None:
            self.client = feast.Client(self.options)
            return
        logging.info("The instance was already created, no-op")

    def apply(self, 
            to_apply: typing.Union[typing.List[
                typing.Union[feast.Entity, feast.FeatureTable]], 
                feast.Entity, feast.FeatureTable],
            projects: str = None) -> None:
        self._check_connected(throw=True)
        self.client.apply(to_apply, projects)

    def ingest(self, ft: feast.FeatureTable, df: pd.DataFrame):
        self.client.ingest(ft, df)
        
    def get_historical_features(self, feature_refs: typing.List[str], entity_source: pd.DataFrame):
        self._check_connected(throw=True)
        return self.client.get_historical_features(feature_refs, entity_source)
        
