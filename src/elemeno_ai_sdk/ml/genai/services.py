import requests

from typing import Dict, List, BinaryIO, Tuple
import io

import pandas as pd

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


class AutoFeatures(InferenceService):

    def featurize(
            self,
            data: str = None, 
            filename: str = None,
            route: str = "/csv/?filename=df.csv",
            ) -> Dict:

        data = {"history": ''}
        with open(filename, "rb") as bfile:
            files = {"file": (filename, bfile)}
            response = self.infer(data=data, files=files, route=route)
        return response.json() 
    
    def parse(self, response) -> Tuple[pd.DataFrame, str, List[str]]:
        df = pd.read_json(io.StringIO(response["data"]))
        return df, response["output"], response["logs"]