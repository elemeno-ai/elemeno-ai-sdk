from typing import Any, Dict, Optional, List, Union, BinaryIO
import os

import aiohttp
import requests

from elemeno_ai_sdk.utils import mlhub_auth
from elemeno_ai_sdk.ml.mlhub_client import MLHubRemote

class GenAI(MLHubRemote):
    GENERATIVE_ROUTE = "/generative/project"

    def __init__(self, env: Optional[str] = None) -> None:
        super().__init__(env=env)
        self.url = f"{self.base_url}/generative"

        api_key = os.environ["MLHUB_API_KEY"]
        header = {"authorization": f"Bearer {api_key}"}
        self.auth_header = header

    def list_projects(self):
        url = f"{self.base_url}/generative/project"
        return requests.get(url=url, headers=self.auth_header)

    def list_servers(self, project_id:str):
        raise NotImplementedError

    # TODO: ver os efeitos do run_id no servidor
    def deploy_server(self, name:str, project_id:str, run_id:str = None):
        url = f"{self.base_url}/generative/project/{project_id}/model-deploy"
        body = {"name": name, "checkpointPath": ""}
        if run_id is not None:
            body["runID"] = run_id
        return requests.post(url=url, data=body, headers=self.auth_header)

    def start_server(self, server_id:str):
        raise NotImplementedError

    def stop_server(self):
        raise NotImplementedError

    #TODO: Get a non streamed version of status
    def stream_server_status(self, server_ids: Union[str, List[str]]):
        url = f"{self.base_url}/inference_server/status"

        if isinstance(server_ids, str):
            server_ids = [server_ids]
        return requests.post(
            url=url,
            data={"inferenceServerIDS":server_ids},
            headers=self.auth_header)

    def request_server(
            self,
            server_id:str,
            body:Dict[str,str] = None, 
            file:Dict[str, BinaryIO] = None, 
            route = "v0/inference"
            ):
        url = f"http://infserv-{server_id}.app.elemeno.ai/{route}"
        return requests.post(url=url, data=body, files=file, headers=self.auth_header)

    @mlhub_auth
    async def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        is_binary: bool = False,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        async with session.get(url=url, params=params) as response:
            if is_binary:
                return await response.content.read()

            return await response.json(content_type=response.content_type)


class AutoFeatures(GenAI):

    def request_server(
            self, 
            server_id: str, 
            body: Dict[str, str] = None, 
            file: Dict[str, BinaryIO] = None, 
            route="csv/",
            ):
        return super().request_server(server_id, body, file, route)