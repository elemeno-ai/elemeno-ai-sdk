from typing import Literal

from elemeno_ai_sdk.ml.genai.protocols.environment import Environment
from elemeno_ai_sdk.ml.genai.protocols.http_adapter import HttpAdapter
from elemeno_ai_sdk.ml.genai.protocols.finetune_interface import IFineTune
from elemeno_ai_sdk.ml.genai.helpers.project_manager import ProjectManager
from elemeno_ai_sdk.ml.genai.entities.finetune import FineTune


class FineTuneManager:
    __env: Environment = None
    __http_adapter: HttpAdapter = None
    __list_finetune: [FineTune] = None
    __project_manager: ProjectManager = None

    def __init__(
        self,
        env: Environment,
        http_adapter: HttpAdapter,
        project_manager: ProjectManager,
    ) -> None:
        self.__env = env
        self.__http_adapter = http_adapter
        self.__project_manager = project_manager
        self.__list_finetune = []

        self.refresh()

    def refresh(self) -> None:
        self.__list_finetune = []
        for project in self.__project_manager.find_all():
            url = self.__env.make_url(f"project/{project}/fine-tune")
            response = self.__http_adapter.get(url, self.__env.headers)
            if "resources" in response:
                for finetune in response["resources"]:
                    self.__list_finetune.append(FineTune(finetune))

    def fit(self, project_id: str, fine_tune: IFineTune) -> None:
        url = self.__env.make_url(
            f"project/{project_id}/{fine_tune.get_endpoint()}"
        )

        self.__http_adapter.post(
            url=url,
            headers=self.__env.headers,
            body=fine_tune.get_body(),
            file=fine_tune.get_file(),
        )

    def find_by_project(self, project_id: str) -> [FineTune]:
        return [x for x in self.__list_finetune if x.project_id == project_id]

    def find_by_status(
        self, status: Literal["RUNNING", "PENDING"]
    ) -> [FineTune]:
        return [x for x in self.__list_finetune if x.status == status]

    def __str__(self) -> str:
        if len(self.__list_finetune) == 0:
            return "No FineTune found."
        return str([str(x) for x in self.__list_finetune])
