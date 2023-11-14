from typing import Any, Dict, Optional

import aiohttp

from elemeno_ai_sdk.ml.mlhub_client import MLHubRemote


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

        file = open(filepath, "rb")
        file_form = aiohttp.FormData()
        file_form.add_field(
            'dataset',
            file,
            content_type='multipart/form-data',
            )

        response = await self.post(url=url, file=file_form)
        file.close()
        return response.status

    async def get_result():
        raise NotImplementedError