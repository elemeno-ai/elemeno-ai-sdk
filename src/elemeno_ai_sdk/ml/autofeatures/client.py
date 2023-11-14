from typing import Any, Dict, Optional

import aiohttp

from elemeno_ai_sdk.ml.mlhub_client import MLHubRemote
from elemeno_ai_sdk.utils import mlhub_auth


class AutoFeaturesClient(MLHubRemote):
    AUTO_FEATURES_REF = "YWktZmVhdHVyZS1lbmdpbmVlcmluZw=="
    
    def __init__(self, env: Optional[str] = None) -> None:
        super().__init__(env=env)

    async def get_job(self, job_id: str):
        url = f"{self.base_url}/script-runner/{job_id}"
        response = await self.get(url=url)
        return response

    async def list_jobs(self):
        url = f"{self.base_url}/script-runner?ref={self.AUTO_FEATURES_REF}"
        return await self.get(url=url)

    async def run_job(self, filepath: str):
        url = f"{self.base_url}/script-runner/{self.AUTO_FEATURES_REF}"

        data = aiohttp.FormData()
        file = open(filepath, "rb")
        data.add_field(
            'dataset',
            file,
            content_type='multipart/form-data',
            )

        response = await self.post_file(url=url, body=data)
        file.close()
        return response.status

    async def get_result():
        raise NotImplementedError
    
    @mlhub_auth
    async def post_file(
        self,
        url: str, 
        body: Dict[str, Any], 
        session: Optional[aiohttp.ClientSession] = None,
        ):
        return await session.post(url, data=body)
