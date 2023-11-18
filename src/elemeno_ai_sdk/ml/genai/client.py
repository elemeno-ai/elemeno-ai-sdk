from typing import Any, Dict, Optional, List, Union, BinaryIO, Tuple
import io
import os

import pandas as pd
import requests

from elemeno_ai_sdk.ml.mlhub_client import MLHubRemote

class InferenceService:
    
    def __init__(self, service_id, api_key: str):
        self._service_id = service_id
        self.base_url = f"http://infserv-{self._service_id}.app.elemeno.ai"

        self._api_key = api_key
        self.auth_header = {
            "authorization": f"Bearer {api_key}",
            "x-api-key": api_key,  
            }


    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError


    def infer(
            self,
            data:Dict[str,str] = None, 
            files:Dict[str, BinaryIO] = None, 
            route = "/v0/inference"
            ):
        url = f"{self.base_url}{route}"
        return requests.post(url, data=data, files=files, headers=self.auth_header)

class Project:

    def __init__(self, base_url: str, api_key:str, project_id:str) -> None:
        self.base_url = base_url
        self._api_key = api_key
        self.auth_header = {"x-api-key": self._api_key} 
        self._project_id = project_id

    def create(self, name:str):
        url = f"{self.base_url}/{self._project_id}/model-deploy"
        body = {"name": name, "checkpointPath": ""}
        return requests.post(url=url, data=body, headers=self.auth_header)

    def get(self):
        url = f"{self.base_url}/{self._project_id}/model-deploy?offset=0"
        return requests.get(url=url, headers=self.auth_header).json()["resources"]

    def delete(self):
        raise NotImplementedError
    
    def services(self) -> Dict[str, InferenceService]:
        service_ids = [resource["id"] for resource in self.get()]
        return {service_id: InferenceService(service_id=service_id, api_key=self._api_key) for service_id in service_ids}


class Resources:

    def __init__(self, base_url, api_key:str):
        self.url = f"{base_url}/project"
        self._api_key = api_key
        self.auth_header = {
            "authorization": f"Bearer {api_key}",
            "x-api-key": api_key,  
            }

    def create(self, name:str, model_type: str):
        data = {"name": name, "modelID": model_type} 
        return requests.post(url=self.url, data=data, headers=self.auth_header)

    def get(self):
        return requests.get(url=self.url, headers=self.auth_header).json()["resources"]

    def delete(self, project_id:str):
        url = f"{self.url}/{project_id}"
        return requests.delete(url=url, headers=self.auth_header).json()

    def projects(self) -> Dict[str, Project]:
        project_ids = [resource["id"] for resource in self.get()]
        return {project_id: Project(self.url, api_key=self._api_key, project_id=project_id) for project_id in project_ids}



class GenAI(MLHubRemote):

    def __init__(self, api_key: str = None, env: Optional[str] = None,) -> None:
        self.url = f"{self.base_url}/generative"

        self._api_key = api_key if api_key else os.environ["MLHUB_API_KEY"]
        assert self._api_key
        self.auth_header = {
            "authorization": f"Bearer {self._api_key}",
            "x-api-key": self._api_key,  
            }
        self.resources = Resources(base_url=self.url, api_key=self._api_key)
    
    def supported_models(self):
        url = f"{self.url}/supported-model"
        response = requests.get(url=url, headers=self.auth_header).json()
        return [resource["id"] for resource in response["resources"]]

    def status(self, service_ids):
        url = f"{self.base_url}/inference_server/status"
        headers = self.auth_header
        
        if isinstance(service_ids, str):
            service_ids = [service_ids]
        data = {"inferenceServerIDS": service_ids}

        with requests.post(url, json=data, headers=headers, stream=True, allow_redirects=True) as response:
            print(response.json())

class AutoFeatures(InferenceService):

    def featurize(
            self,
            data: str = None, 
            filename: str = None,
            route: str = "csv/?filename=df.csv",
            ) -> Dict:

        data = {"history": ''}
        with open(filename, "rb") as bfile:
            files = {"file": (filename, bfile)}
            response = self.infer(data=data, files=files, route=route)
        return response.json() 
    
    def parse(self, response) -> Tuple[pd.DataFrame, str, List[str]]:
        df = pd.read_json(io.StringIO(response["data"]))
        return df, response["output"], response["logs"]