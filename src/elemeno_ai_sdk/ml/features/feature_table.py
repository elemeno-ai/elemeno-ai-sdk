import os
from typing import Any, Dict, List, Optional

from elemeno_ai_sdk.logger import logger
from elemeno_ai_sdk.ml.features.schema import FeatureTableSchema
from elemeno_ai_sdk.ml.features.utils import get_feature_server_url_from_api_key
from elemeno_ai_sdk.ml.mlhub_client import MLHubRemote


class FeatureTable(MLHubRemote):
    """A FeatureTable is the object that is used to define feature tables on Elemeno feature store.

    If you're looking to create a new feature table or read data look at ingest_schema of the class FeatureStore.
    """

    def __init__(self, remote_server: Optional[str] = None):
        if remote_server is None:
            api_key = os.getenv("MLHUB_API_KEY")
            if api_key is None:
                raise ValueError("Please set the MLHUB_API_KEY environment variable.")
            self._remote_server = get_feature_server_url_from_api_key(api_key)
        else:
            self._remote_server = remote_server

    async def create(self, schema_path: str) -> None:
        endpoint = f"{self._remote_server}/feature-view"

        table_schema = FeatureTableSchema().load_data(schema_path)
        name = table_schema["name"]
        entities = table_schema["entities"]
        schema = table_schema["schema"]

        body = {"name": name, "entities": entities, "schema": schema}
        try:
            await self.post(url=endpoint, body=body)
            logger.info(f"Feature table {name} created successfully.")
        except Exception:
            logger.exception(f"Failed to create feature table {name}.")

    async def list(self) -> List[Dict[str, Any]]:
        endpoint = f"{self._remote_server}/list-feature-views"
        response = await self.get(url=endpoint)
        return response["feature_views"]

    async def delete(self, ft_name: str) -> None:
        endpoint = f"{self._remote_server}/{ft_name}/delete-feature-view"
        try:
            await self.post(url=endpoint, body={})
            logger.info(f"Deleted feature table {ft_name} successfully.")
        except Exception:
            logger.exception(f"Failed to delete feature table {ft_name}")
