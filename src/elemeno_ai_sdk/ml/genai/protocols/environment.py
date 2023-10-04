import os

from elemeno_ai_sdk.ml.genai.entities.generics import PROD_URL, DEV_URL


class Environment:
    url_base: str
    api_key: str

    def __init__(self, env: str) -> None:
        if env == "prod":
            self.url_base = PROD_URL
        elif env == "dev":
            self.url_base = DEV_URL
        else:
            raise ValueError("Invalid environment. Please use dev or prod.")

        if not os.getenv("API_KEY"):
            raise ValueError(
                "API_KEY environment variable not found or empty."
            )

        self.api_key = os.getenv("API_KEY")

    def make_url(self, endpoint: str) -> str:
        return f"{self.url_base}/generative/{endpoint}"

    @property
    def headers(self) -> str:
        return {
            "x-api-key": self.api_key,
            "Authorization": f"Bearer {self.api_key}",
        }

    @headers.setter
    def headers(self):
        raise AttributeError("Cannot set readonly property: headers.")
