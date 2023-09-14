import logging
from typing import Any, Dict, List

import aiohttp

from src.elemeno_ai_sdk.ml.automl.request import AutoMLRequest
from src.elemeno_ai_sdk.ml.automl.response import (
    GetJobResponse,
    ListJobsResponse,
    RunAutoMLResponse,
)


PROD_URL = "https://c3po.ml.semantixhub.com"
DEV_URL = "https://c3po-stg.ml.semantixhub.com"


class AutoMLClient:
    def __init__(self, env: str, api_key: str) -> None:
        self.env = env
        self.api_key = api_key

    @property
    def base_url(self):
        base_url = None
        if self.env == "prod":
            base_url = PROD_URL
        elif self.env == "dev":
            base_url = DEV_URL
        else:
            raise ValueError("Invalid environment. Please use dev or prod.")
        return base_url

    @property
    def headers(self):
        return {"Content-Type": "application/json", "x-api-key": self.api_key}

    async def _get(self, url: str):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(url=url) as response:
                if not response.ok:
                    logging.exception(
                        f"Failed to start automl job with: \n"
                        f"\t status code= {response.status} \n"
                        f"\t header= {self.headers}"
                    )
                return await response.json()

    async def _post(self, url: str, body: Dict[str, Any]):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.post(url=url, json=body) as response:
                if not response.ok:
                    logging.exception(
                        f"Failed to start automl job with: \n"
                        f"\t status code= {response.status} \n"
                        f"\t message_body= {body} \n"
                        f"\t header= {self.headers}"
                    )

                return await response.json()

    def list_jobs(self) -> List[AutoMLRequest]:
        response = self._get(url=f"{self.base_url}/automl")
        return ListJobsResponse(**response).resources

    def get_job(self, job_id: str) -> GetJobResponse:
        response = self._get(url=f"{self.base_url}/automl/{job_id}")
        return GetJobResponse(**response)

    def run_job(
        self,
        feature_table_name: str,
        features_selected: str,
        id_column: str,
        target_name: str,
        start_date: str,
        end_date: str,
        task: str,
        scoring: str,
        num_features: int,
        generations: int,
    ) -> RunAutoMLResponse:
        body = AutoMLRequest(
            featureTableName=feature_table_name,
            featureSelected=features_selected,
            idColumn=id_column,
            targetName=target_name,
            startDate=start_date,
            endDate=end_date,
            task=task,
            scoring=scoring,
            numFeatures=num_features,
            generations=generations,
        ).to_dict()

        response = self._post(url=f"{self.base_url}/automl", body=body)

        return RunAutoMLResponse(job_id=response.get("id", None))
