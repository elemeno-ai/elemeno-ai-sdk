import json
from typing import Any, Dict, List

from elemeno_ai_sdk.ml.mlhub_client import MLHubRemote


class FeatureTable:
    """A FeatureTable is the object that is used to define feature tables on Elemeno feature store.

    If you're looking to create a new feature table or read data look at ingest_schema of the class FeatureStore.
    """

    def __init__(self, remote_server: str, schema_path: str, timestamp_col: str = "event_timestamp"):
        self._mlhub_client = MLHubRemote()
        self._remote_server = remote_server
        self.schema_path = schema_path
        self.timestamp_col = timestamp_col

    @property
    def ft_name(self) -> str:
        return self._table_schema.get("name")

    @property
    def entities(self) -> List[str]:
        return self._table_schema.get("entities")

    def _validate_schema(self, table_schema: Dict[str, Any]):
        features = [feat["name"] for feat in table_schema["schema"]]
        if self.timestamp_col not in features:
            raise ValueError(f"{self.timestamp_col} must be in the table schema with type timestamp")

    @property
    def table_schema(self) -> Dict[str, Any]:
        with open(self.schema_path, "r") as schema:
            table_schema = json.load(schema)

        self._validate_schema(table_schema)

        return table_schema

    @entities.setter
    def entities(self, entities: List[str]):
        self._entities = entities

    @property
    def features(self):
        return self._table_schema.get("schema")

    @features.setter
    def features(self, features: List[Dict[str, Any]]):
        self._features = features

    def set_table_schema(self, table_schema: Dict[str, Any]) -> None:
        self._table_schema = table_schema

    async def create(self) -> None:
        endpoint = f"{self._remote_server}/feature-view"

        body = {"name": self.ft_name, "entities": self.entities, "schema": self.features}
        return await self._mlhub_client.post(url=endpoint, body=body)

    async def list(self) -> List[Dict[str, Any]]:
        endpoint = f"{self._remote_server}/list-feature-views"
        response = await self._mlhub_client.get(url=endpoint)
        return response["feature_views"]

    async def delete(self):
        endpoint = f"{self._remote_server}/{self.ft_name}/list-feature-views"
        return await self._mlhub_client.post(url=endpoint)
