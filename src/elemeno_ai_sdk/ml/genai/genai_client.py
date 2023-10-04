from elemeno_ai_sdk.ml.genai.helpers.model_manager import ModelManager
from elemeno_ai_sdk.ml.genai.protocols.environment import Environment
from elemeno_ai_sdk.ml.genai.protocols.http_adapter import HttpAdapter
from elemeno_ai_sdk.ml.genai.helpers.project_manager import ProjectManager
from elemeno_ai_sdk.ml.genai.helpers.deploy_manager import DeployManager
from elemeno_ai_sdk.ml.genai.helpers.finetune_manager import FineTuneManager
from elemeno_ai_sdk.ml.genai.playground.playground import Playground


class GenAIClient:
    __environment: Environment = None
    __http_adapter: HttpAdapter = None
    __model_manager: ModelManager = None
    __project_manager: ProjectManager = None
    __deploy_manager: DeployManager = None
    __fine_tune: FineTuneManager = None
    __playground: Playground = None

    def __init__(self, env: str = "dev") -> None:
        self.__environment = Environment(env)
        self.__http_adapter = HttpAdapter()
        self.__model_manager = ModelManager(
            self.__environment, self.__http_adapter
        )
        self.__project_manager = ProjectManager(
            self.__environment, self.__http_adapter, self.__model_manager
        )

        self.__deploy_manager = DeployManager(
            self.__environment,
            self.__http_adapter,
            self.__model_manager,
            self.__project_manager,
        )

        self.__fine_tune = FineTuneManager(
            self.__environment, self.__http_adapter, self.__project_manager
        )

        self.__playground = Playground(self.__environment, self.__http_adapter)

    @property
    def models(self):
        return self.__model_manager

    @property
    def projects(self):
        return self.__project_manager

    @property
    def deploys(self):
        return self.__deploy_manager

    @property
    def finetune(self):
        return self.__fine_tune

    @property
    def playground(self):
        return self.__playground
