from typing import Any, Dict, Optional

import aiohttp
import requests

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

    async def get_inf_status(self):
        inf_id = "6526f214d97b26ec1eb19239"
        route = "/resource-status"
        url = f"http://infserv-{inf_id}.app.elemeno.ai{route}"
        return await self.get(url=url)
        ## return await self.post(url=url, body={"instances": [{"prompt": "What are you doing"}]})
    

    def postit(self,):
        inf_id = "6515c62eeb094d57a99eed28"
        url = self.base_url+"/generative/supported-model"
        ## url = f"https://infserv-{inf_id}.app.elemeno.ai/v0/inference"
        api_key = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImVsZW1lbm8tc2Fhcy1hcHBzLWtleS1pZCIsInR5cCI6IkpXVCJ9.eyJhY2NvdW50Ijoid2FnbmVyLnJ1cHBAc2VtYW50aXguY29tLmJyIiwic3ViIjoiZGV2ZWxvcG1lbnRAZWxlbWVuby5haSIsImlhdCI6MTY5OTU2NDg2NywiZXhwIjoxNzA1MzUzNjU3LCJpc3MiOiJkZXZlbG9wbWVudEBlbGVtZW5vLmFpIn0.ZoHWprbnzw89I5653HhTe-qD-GI4xSegjzKhIH4_iBLlNaSzS4-JTnMQUSchWZUA4f3SmIfyrdVGVnu1OmNvSQDxZ1RlANHzpTyPtYghxNOBDSsbwCEqrQXeWsFlCocWsdyfeayab87GCGq7zHgj6qQVs1r-MjK5qozZfR96iEosXhwNG7cPihOD6bnMG5yVZ1OAELfgCo_ViOSKUGMz4BuZ4uunYPMZan62Y166mfNSH4Q4v_eyAS02heMoWMkev0AIfZId64dLzbHG7T2P2klmb2auAm85dPhus8iS0hg0lvPPALRBpW_Sa3Nq_yKc7KNaR3szjdhbh2vx9erD1w"
        return requests.get(
            url=url,
            headers={
                "Content-type": "application/json",
                "authorization": f"Bearer {api_key}"

            }
        )