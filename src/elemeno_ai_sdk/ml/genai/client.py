from typing import Any, Dict, Optional, List, Union, BinaryIO, Tuple
import io
import os

import aiohttp
import pandas as pd
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
    def server_status(self, server_ids: Union[str, List[str]]):
        raise NotImplementedError
        ### url = f"{self.base_url}/inference_server/status"

        ### if isinstance(server_ids, str):
            ### server_ids = [server_ids]
        ### return requests.post(
            ### url=url,
            ### data={"inferenceServerIDS":server_ids},
            ### headers=self.auth_header)

    def request_server(
            self,
            server_id:str,
            body:Dict[str,str] = None, 
            file:Dict[str, BinaryIO] = None, 
            route = "v0/inference"
            ):
        url = f"http://infserv-{server_id}.app.elemeno.ai/{route}"
        headers = self.auth_header | {'accept': 'application/json'}
        return requests.post(url=url, data=body, files=file, headers=headers)

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

    def featurize(
            self,
            server_id: str,
            filename: str,            
            ) -> Dict:
        """
        Featurize the data in the specified CSV file by sending it to a auto-features service.
        The function sends the specified CSV file to the remote server identified by 'server_id'
        and retrieves the featurized data along with additional information and logs.

        Parameters:
        - server_id (str): The unique identifier of the remote server where the featurization will be performed.
        - filename (str): The path to the CSV file containing the data to be featurized.

        Returns:
        - Dict: the server response in dictionary form. 

        Example:
        ```
        server_id = "50498772a344a63346c1223d5"
        filename = "path/to/data.csv"
        client = AutoFeatures(env="dev")
        response = client.featurize(server_id, filename)
        data, output, logs = client.parse(reponse)
        print("Featurized Data:", data)
        print("Output:", output)
        print("Logs:", logs)
        ```
        """
        route = "csv/?filename=df.csv"
        body = {"history": ''}
        with open(filename, "rb") as bfile:
            files = {"file": (filename, bfile)}
            response = self.request_server(server_id, body, files, route)
        return response.json() 
    
    def parse(self, response) -> Tuple[pd.DataFrame, str, List[str]]:
        """
        Helper function to parse the server response into DataFrame, output and logs

        Returns:
        Tuple: A tuple containing the featurization results.
            - df (pd.Dataframe): The featurized dataframe obtained from the server.
            - output (str): Additional output or information related to the featurization process.
            - logs (str): Logs or messages generated during the featurization process.

        Example:
        ```
        server_id = "50498772a344a63346c1223d5"
        filename = "path/to/data.csv"
        client = AutoFeatures(env="dev")
        response = client.featurize(server_id, filename)
        data, output, logs = client.parse(reponse)
        print("Featurized Data:", data)
        print("Output:", output)
        print("Logs:", logs)
        ```
        """
        df = pd.read_json(io.StringIO(response["data"]))
        return df, response["output"], response["logs"]