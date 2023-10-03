import asyncio
import json
from asyncio import Semaphore
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm import trange

from elemeno_ai_sdk.ml.mlhub_client import MLHubRemote


class FeatureStore:
    def __init__(self, remote_server: str):
        self._remote_server = remote_server
        self._mlhub_client = MLHubRemote()

    async def ingest(
        self,
        feature_table_name: str,
        to_ingest: pd.DataFrame,
        renames: Optional[Dict[str, str]] = None,
        all_columns: Optional[List[str]] = None,
    ) -> None:
        """
        Ingests data into a feature table

        args:

        - feature_table: FeatureTable instance
        - to_ingest: Data to ingest
        - renames: Renames to apply to the data
        - all_columns: List of columns to ingest

        return:

        - None
        """
        endpoint = f"{self._remote_server}/{feature_table_name}/push"

        # adjust the column names
        if renames is not None:
            to_ingest = to_ingest.rename(columns=renames)

        # filter the columns
        if all_columns is not None:
            to_ingest = to_ingest[all_columns]

        # paginate the insertion, 500 rows of to_ingest at a time
        for i in trange(0, len(to_ingest), 500):
            data = to_ingest.iloc[i : i + 500].to_dict("list")
            body = {"df": data, "to": "online_and_offline"}
            await self._mlhub_client.post(url=endpoint, body=body)

    async def get_training_features(
        self,
        feature_table_name: str,
        entities: List[str] = None,
        features: List[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Gets training features from a feature table

        args:

        - feature_table: FeatureTable instance
        - entities: List of entity names to select
        - features: List of feature names to select
        - date_from: Start date
        - date_to: End date

        return:

        - pd.DataFrame
        """

        endpoint = f"{self._remote_server}/{feature_table_name}/historical-features"

        params = {"initial_date": date_from, "end_date": date_to}
        if entities is not None:
            params["entities"] = json.dumps(entities)
        if features is not None:
            params["feature_refs"] = json.dumps(features)
        # request all pages, after the first request it will get all the other pages in parallel
        response = await self._retrieve_pages_in_parallel(endpoint, params)
        return pd.DataFrame([row for page in response for row in page])

    async def _retrieve_pages_in_parallel(self, endpoint, params, page_size=100, max_concurrent_requests=10):
        # Use aiohttp client session to make the first request and get total pages
        params["page_size"] = page_size
        params["page"] = 1
        response = await self._mlhub_client.get(endpoint, params)
        total_pages = response["pagination"]["total_pages"]
        # If there's only one page, return the response immediately
        if total_pages == 1:
            return [response["data"]]

        # Create a semaphore to limit the number of concurrent requests
        semaphore = Semaphore(max_concurrent_requests)

        # Fetch all pages in parallel
        tasks = [self._fetch_page(semaphore, endpoint, {**params, "page": page}) for page in range(2, total_pages + 1)]
        pages = await asyncio.gather(*tasks)
        pages = [page["data"] for page in pages]

        return [response["data"]] + pages

    async def _fetch_page(self, semaphore, url, params):
        async with semaphore:
            return await self._mlhub_client.get(url, params)

    async def get_online_features(self, feature_table_name: str, entities: Dict[str, List], features: List[str]):
        endpoint = f"{self._remote_server}/{feature_table_name}/online-features"

        qentities = [{"entity": k, "value": v} for k, v in entities.items()]
        params = {"entities": json.dumps(qentities), "feature_refs": json.dumps(features)}
        return await self._mlhub_client.get(endpoint, params)

    async def create_feature_table(self, fs_schema: str):
        endpoint = f"{self._remote_server}/feature-view"

        with open(fs_schema, "r") as schema:
            fs_schema = json.load(schema)

        body = {"name": fs_schema["name"], "entities": fs_schema["entities"], "schema": fs_schema["schema"]}
        return await self._mlhub_client.post(url=endpoint, body=body)
