from elemeno_ai_sdk.ml.genai.entities.models import Model
from elemeno_ai_sdk.ml.genai.protocols.environment import Environment
from elemeno_ai_sdk.ml.genai.protocols.http_adapter import HttpAdapter


class ModelManager:
    __list_models: [Model]

    def __init__(self, env: Environment, http_adapter: HttpAdapter) -> None:
        url = env.make_url("supported-model")
        response = http_adapter.get(url, headers=env.headers)
        self.__list_models = [Model(x) for x in response["resources"]]

    def get(self, model_name: str) -> Model:
        for x in self.__list_models:
            if x.model_name == model_name:
                return x
        raise ValueError(f"Model {model_name} not found.")

    def __str__(self) -> str:
        if len(self.__list_models) == 0:
            return "No Models found."
        return str([x.model_name for x in self.__list_models])
