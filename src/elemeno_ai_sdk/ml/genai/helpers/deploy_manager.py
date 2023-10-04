from elemeno_ai_sdk.ml.genai.entities.deploy import Deploy
from elemeno_ai_sdk.ml.genai.helpers.project_manager import ProjectManager
from elemeno_ai_sdk.ml.genai.helpers.model_manager import ModelManager
from elemeno_ai_sdk.ml.genai.protocols.environment import Environment
from elemeno_ai_sdk.ml.genai.protocols.http_adapter import HttpAdapter


class DeployManager:
    __list_deploy: [Deploy] = []
    __env: Environment = None
    __http_adapter: HttpAdapter = None
    __model_manager: ModelManager = None
    __project_manager: ProjectManager = None

    def __init__(
        self,
        env: Environment,
        http_adapter: HttpAdapter,
        model_manager: ModelManager,
        project_manager: ProjectManager,
    ) -> None:
        self.__env = env
        self.__http_adapter = http_adapter
        self.__model_manager = model_manager
        self.__project_manager = project_manager

        self.refresh()

    def refresh(self) -> None:
        self.__list_deploy = []
        for project in self.__project_manager.find_all():
            url = self.__env.make_url(
                f"project/{project}/model-deploy?offset=0"
            )
            response = self.__http_adapter.get(url, self.__env.headers)
            if response["resources"] is not None:
                for deploy in response["resources"]:
                    self.__list_deploy.append(Deploy(deploy))

    def __str__(self) -> str:
        if len(self.__list_deploy) == 0:
            return "No Deploys found."
        return str([x.name + "-" + x.project_id for x in self.__list_deploy])

    def deploy(self, project_id: str, deploy_name: str, model_name: str):
        try:
            url = self.__env.make_url(f"project/{project_id}/model-deploy")
            data = {
                "name": deploy_name,
                "checkpointPath": self.__model_manager.get(model_name)
                .ptweights[0]
                .path,
            }
            self.__http_adapter.post(
                url=url, headers=self.__env.headers, body=data
            )
            print(f"Deploy {deploy_name} created")
        except Exception as e:
            raise ValueError("Erro add deploy: " + str(e))

    def find_by_name(self, name: str) -> Deploy:
        for deploy in self.__list_deploy:
            if deploy.name in name:
                return deploy
        return None

    def find_by_project(self, project_id: str) -> Deploy:
        for deploy in self.__list_deploy:
            if deploy.project_id == project_id:
                return deploy
        return None
