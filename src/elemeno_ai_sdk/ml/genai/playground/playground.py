from elemeno_ai_sdk.ml.genai.protocols.environment import Environment
from elemeno_ai_sdk.ml.genai.protocols.http_adapter import HttpAdapter


class Playground:
    __env: Environment
    __http_adapter: HttpAdapter

    def __init__(self, env: Environment, http_adapter: HttpAdapter):
        self.__env = env
        self.__http_adapter = http_adapter

    def prompt(self, deploy_id: str, prompt: str) -> str:
        url = self.__env.make_url(
            f"https://infserv-{deploy_id}.app.elemeno.ai/v0/inference"
        )
        data = {"prompt": [prompt]}

        return self.__http_adapter.post(
            url, headers=self.__env.headers, data=data
        )
