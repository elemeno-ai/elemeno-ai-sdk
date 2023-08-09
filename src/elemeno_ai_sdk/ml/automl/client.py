import logging
from typing import Dict

import aiohttp


class AutoMLClient:
    URL = {
        "dev": "https://c3po-stg.ml.semantixhub.com",
        "prod": "https://c3po.ml.semantixhub.com",
    }

    def __init__(self, env: str, api_key: str) -> None:
        self.env = env
        self.api_key = api_key

    @property
    def base_url(self):
        try:
            return self.URL[self.env]
        except KeyError:
            logging.exception("Invalid environment. Please use dev or prod.")

    @property
    def headers(self):
        return {"Content-Type": "application/json", "x-api-key": self.api_key}

    async def list_jobs(self):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(url=f"{self.base_url}/automl") as response:
                if not response.ok:
                    logging.exception(
                        f"Failed to start automl job with: \n"
                        f"\t status code= {response.status} \n"
                        f"\t header= {self.headers}"
                    )
                return await response.json()

    async def run_job(
        self,
        experiment_id: str,
        feature_table_name: str,
        id_column: str,
        target_name: str,
        start_date: str,
        end_date: str,
        task: str,
        scoring: str,
        num_features: int,
        generations: int,
    ) -> Dict[str, str]:
        body = {
            "experimentID": experiment_id,
            "featureTableName": feature_table_name,
            "idColumn": id_column,
            "targetName": target_name,
            "startDate": start_date,
            "endDate": end_date,
            "task": task,
            "scoring": scoring,
            "numFeatures": num_features,
            "generations": generations,
        }
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.post(
                url=f"{self.base_url}/automl", json=body
            ) as response:
                if not response.ok:
                    logging.exception(
                        f"Failed to start automl job with: \n"
                        f"\t status code= {response.status} \n"
                        f"\t message_body= {body} \n"
                        f"\t header= {self.headers}"
                    )

                return await response.json()

    async def get_job(self, job_id: str):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(url=f"{self.base_url}/automl/{job_id}") as response:
                if not response.ok:
                    logging.exception(
                        f"Failed to start automl job with: \n"
                        f"\t status code= {response.status} \n"
                        f"\t header= {self.headers}"
                    )
                return await response.json()
