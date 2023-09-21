import aiohttp
import asyncio
import json
from datetime import datetime
import pandas as pd
from typing import Optional, Dict, List

from elemeno_ai_sdk.utils import mlhub_auth
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable

class FeatureStore:

    def __init__(self, remote_server: str):
        self._remote_server = remote_server

    async def ingest(
        self,
        feature_table: FeatureTable,
        to_ingest: pd.DataFrame,
        renames: Optional[Dict[str, str]] = None,
        all_columns: Optional[List[str]] = None
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
        endpoint = f"{self._remote_server}/{feature_table.name}/push"

        # adjust the column names
        if renames is not None:
            to_ingest = to_ingest.rename(columns=renames)
        
        # filter the columns
        if all_columns is not None:
            to_ingest = to_ingest[all_columns]

        # paginate the insertion, 500 rows of to_ingest at a time
        for i in range(0, len(to_ingest), 500):
            data = to_ingest.iloc[i:i+500].to_dict("list")
            
            body = {
                "df": data,
                "to": "online_and_offline"
            }
            await self._ingest_remote(endpoint, body)

    @mlhub_auth
    async def _ingest_remote(self, endpoint, body, session: aiohttp.ClientSession = None):
        headers = {"Content-Type": "application/json"}
        async with session.post(url=endpoint, data=json.dumps(body), headers=headers) as response:
            if not response.ok:
                raise ValueError(
                    f"Failed to ingest data with: \n"
                    f"\t status code= {response.status} \n"
                    f"\t message_body= {body} \n"
                )
            return await response.text()

    async def get_training_features(
        self,
        feature_table: FeatureTable,
        entities: List[str] = None,
        features: List[str] = None,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None
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

        endpoint = f"{self._remote_server}/{feature_table.name}/historical-features"

        params = {
            "initial_date": date_from.strftime("%Y-%m-%d"),
            "end_date": date_to.strftime("%Y-%m-%d")
        }
        if entities is not None:
            params["entities"] = json.dumps(entities)
        if features is not None:
            params["feature_refs"] = json.dumps(features)
        # request all pages, after the first request it will get all the other pages in parallel
        response = await self._retrieve_pages_in_parallel(endpoint, params)
        return pd.DataFrame([row for page in response for row in page])
            
        
    async def _fetch_page(self, session, endpoint, params):
        async with session.get(endpoint, params=params) as response:
            return await response.json()

    @mlhub_auth
    async def _retrieve_pages_in_parallel(self, endpoint, params, page_size=100, session: aiohttp.ClientSession = None):
        # Use aiohttp client session to make the first request and get total pages
        params["page_size"] = page_size
        params["page"] = 1
        response = await self._fetch_page(session, endpoint, params)
        total_pages = response['pagination']['total_pages']
        # If there's only one page, return the response immediately
        if total_pages == 1:
            return [response['data']]

        # Fetch all pages in parallel
        tasks = [self._fetch_page(session, endpoint, {**params, "page": page}) for page in range(2, total_pages + 1)]
        pages = await asyncio.gather(*tasks)
        pages = [page['data'] for page in pages]

        return [response['data']] + pages
    
    async def get_online_features(self, feature_table: FeatureTable, entities: Dict[str, List], features: List[str]):
        endpoint = f"{self._remote_server}/{feature_table.name}/online-features"

        qentities = [{"entity": k, "value": v} for k, v in entities.items()]
        params = {
            "entities": json.dumps(qentities),
            "feature_refs": json.dumps(features)
        }
        return await self._get_online_features_remote(endpoint, params)
    
    @mlhub_auth
    async def _get_online_features_remote(self, endpoint, params, session: aiohttp.ClientSession = None):
        headers = {"Content-Type": "application/json"}
        async with session.get(url=endpoint, params=params, headers=headers) as response:
            if not response.ok:
                raise ValueError(
                    f"Failed to get online features with: \n"
                    f"\t status code= {response.status} \n"
                )
            return await response.json()