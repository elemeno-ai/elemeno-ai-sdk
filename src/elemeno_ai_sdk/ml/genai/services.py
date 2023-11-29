from typing import Dict, List, BinaryIO, Tuple, Any
import io

import asyncio
import pandas as pd
import requests

from .chat import chat_socket

class InferenceService:
    
    def __init__(self, service_id: str, api_key: str):
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
            data: Dict[str,str] = None, 
            files: Dict[str, BinaryIO] = None, 
            route: str = "/v0/inference"
            ) -> Any:
        url = f"{self.base_url}{route}"
        return requests.post(url, data=data, files=files, headers=self.auth_header)


class AutoFeatures(InferenceService):
    url = "http://localhost:8080"

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
    
    def parse(self, response: str) -> Tuple[pd.DataFrame, str, List[str]]:
        df = pd.read_json(io.StringIO(response["data"]))
        return df, response["output"], response["logs"]
    
    def chat(
        self,
        filename: str,
        route: str = "/chat"
    ) -> pd.DataFrame:
        url = self.url + route
        url = url.replace("http", "ws")
        
        loop = asyncio.get_event_loop()
        df_bytes = loop.run_until_complete(chat_socket(
            file_path=filename, 
            websocket_uri=url,
            headers=self.auth_header,
            ))
        return pd.read_csv(df_bytes)

