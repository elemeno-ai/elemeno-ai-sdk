from typing import Any
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
        return self.URL[self.env]

    @property
    def headers(self):
        return {"Content-Type": "application/json", "x-api-key": self.api_key}

    async def list_jobs(self):
        async with aiohttp.ClientSession(
            base_url=self.base_url, headers=self.headers
        ) as session:
            response = await session.get(url="/automl")

            if response.status != 200:
                print(f"Failed to list automl jobs with status code: {response.status}")

            data = await response.json()
            return data

    async def run_job(self, data: Dict[str, Any]) -> Dict[str, str]:
        async with aiohttp.ClientSession(
            base_url=self.base_url, headers=self.headers
        ) as session:
            response = await session.post(url="/automl", data=data)

            if response.status != 200:
                print(f"Failed to start automl job with status code: {response.status}")

            data = await response.json()
            return data

    async def get_job(self, job_id: str):
        async with aiohttp.ClientSession(
            base_url=self.base_url, headers=self.headers
        ) as session:
            response = await session.get(url=f"/automl/{job_id}")

            if response.status != 200:
                print(
                    f"Failed to get automl job with job_id {job_id} and status code: {response.status}"
                )

            data = await response.json()
            return data
