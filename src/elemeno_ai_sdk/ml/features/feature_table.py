import os
from typing import Any, Dict, List, Optional

from elemeno_ai_sdk.ml.features.schema import FeatureTableSchema
from elemeno_ai_sdk.ml.features.utils import get_feature_server_url_from_api_key
from elemeno_ai_sdk.ml.mlhub_client import MLHubRemote


class FeatureTable(MLHubRemote):
    """A FeatureTable is the object that is used to define feature tables on Elemeno feature store.

    If you're looking to create a new feature table or read data look at ingest_schema of the class FeatureStore.
    """

    def __init__(self, schema_path: str, remote_server: Optional[str] = None):
        if remote_server is None:
            api_key = os.getenv("MLHUB_API_KEY")
            if api_key is None:
                raise ValueError("Please set the MLHUB_API_KEY environment variable.")
            self._remote_server = get_feature_server_url_from_api_key(api_key)
        else:
            self._remote_server = remote_server
        self._schema_path = schema_path

    @property
    def name(self) -> str:
        return self.table_schema.get("name")

    @property
    def entities(self) -> List[str]:
        return self.table_schema.get("entities")

    @property
    def features(self) -> List[Dict[str, str]]:
        return self.table_schema.get("schema")

    @property
    def table_schema(self):
        return FeatureTableSchema().load_data(self._schema_path)

    async def create(self) -> None:
        endpoint = f"{self._remote_server}/feature-view"

        body = {"name": self.name, "entities": self.entities, "schema": self.features}
        return await self.post(url=endpoint, body=body)

    async def list(self) -> List[Dict[str, Any]]:
        endpoint = f"{self._remote_server}/list-feature-views"
        response = await self.get(url=endpoint)
        return response["feature_views"]

    async def delete(self, ft_name: str) -> None:
        endpoint = f"{self._remote_server}/{ft_name}/delete-feature-view"
        return await self.post(url=endpoint, body={})
