import logging
import aiohttp
from elemeno_ai_sdk.utils import mlhub_auth, MLHubRemote
from typing import Any, Dict

class AutoMLClient:
    def __init__(self, env: str = "prod") -> None:
        self.env = env

    @mlhub_auth
    async def _get(self, url: str, session: aiohttp.ClientSession = None):
        async with session.get(url=url) as response:
            if not response.ok:
                logging.exception(
                    f"Failed to start automl job with: \n"
                    f"\t status code= {response.status} \n"
                    f"\t header= {self.headers}"
                )
            return await response.json()

    @mlhub_auth
    async def _post(self, url: str, body: Dict[str, Any], session: aiohttp.ClientSession = None):
        async with session.post(url=url, json=body) as response:
            if not response.ok:
                logging.exception(
                    f"Failed to start automl job with: \n"
                    f"\t status code= {response.status} \n"
                    f"\t message_body= {body} \n"
                    f"\t header= {self.headers}"
                )

            return await response.json()

    def list_jobs(self):
        base_url = MLHubRemote(env=self.env).base_url
        return self._get(url=f"{base_url}/automl")

    def get_job(self, job_id: str):
        base_url = MLHubRemote(env=self.env).base_url
        return self._get(url=f"{base_url}/automl/{job_id}")

    def run_job(
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
        return self._post(url=f"{self.base_url}/automl", body=body)
