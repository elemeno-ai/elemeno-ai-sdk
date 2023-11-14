from typing import Optional

import aiohttp

from elemeno_ai_sdk.ml.mlhub_client import MLHubRemote

class DataSource(MLHubRemote):
    
    def __init__(self, env: Optional[str] = None) -> None:
        super().__init__(env=env)
        self.url = f"{self.base_url}/datasource"

    async def list_sources(self):
        return await self.get(url=self.url)

    async def upload_csv(self, filepath: str, description: str):
        url = f"{self.base_url}/datasource"

        file = open(filepath, "rb")
        data = aiohttp.FormData()
        data.add_field(
            'file',
            file,
            content_type='multipart/form-data',
            )
        data.add_field("type", "CSV")
        data.add_field("description", description)

        response = await self.post(url=url, file=data)
        file.close()
        return response.status