import requests
from typing import Dict

from elemeno_ai_sdk.ml.genai.entities.generics import JSON


class HttpAdapter:
    def get(self, url: str, headers: Dict) -> JSON:
        try:
            response = requests.get(url=url, headers=headers)
            if response.status_code == 200:
                return response.json()

        except requests.exceptions.RequestException as e:
            raise ValueError(f"HTTP Get request failed: {e}")

        raise ValueError(
            f"HTTP Get request failed Status Code: {response.status_code}"
        )

    def post(self, url: str, headers: Dict, body: JSON, file=None) -> JSON:
        try:
            if file is None:
                response = requests.post(url=url, json=body, headers=headers)
            else:
                response = requests.post(
                    url=url, json=body, headers=headers, files=file
                )

            if response.status_code in (200, 201):
                return response.json()

        except requests.exceptions.RequestException as e:
            raise ValueError(f"HTTP Post request failed: {e}")

        raise ValueError(
            f"HTTP Post request failed Status Code: {response.status_code}"
        )

    def delete(self, url: str, headers: Dict) -> JSON:
        try:
            response = requests.delete(url=url, headers=headers)
            if response.status_code in (200, 201):
                return response.json()

        except requests.exceptions.RequestException as e:
            raise ValueError(f"HTTP Delete request failed: {e}")

        raise ValueError(
            f"HTTP Delete request failed Status Code: {response.status_code}"
        )
