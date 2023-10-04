from datetime import datetime
from typing import Dict


from elemeno_ai_sdk.ml.genai.entities.project import Project
from elemeno_ai_sdk.ml.genai.helpers.model_manager import ModelManager
from elemeno_ai_sdk.ml.genai.protocols.environment import Environment
from elemeno_ai_sdk.ml.genai.protocols.http_adapter import HttpAdapter


class ProjectManager:
    __list_projects: [Project] = []
    __env: Environment = None
    __http_adapter: HttpAdapter = None

    def __init__(
        self,
        env: Environment,
        http_adapter: HttpAdapter,
        model_manager: ModelManager,
    ) -> None:
        self.__env = env
        self.__http_adapter = http_adapter
        self.__model_manager = model_manager

        self.refresh()

    def refresh(self) -> None:
        url = self.__env.make_url("project")
        response = self.__http_adapter.get(url, headers=self.__env.headers)
        self.__list_projects = []
        if "resources" in response:
            for project in response["resources"]:
                self.__list_projects.append(Project(project))

    def create(
        self, project_name: str, model_name: str, secrets: Dict = None
    ) -> str:
        data = {
            "modelId": self.__model_manager.get(model_name).id,
            "name": project_name,
        }
        if secrets is not None:
            data["secrets"] = secrets

        try:
            url = self.__env.make_url("project")
            response = self.__http_adapter.post(
                url=url, headers=self.__env.headers, body=data
            )
            self.__list_projects.append(Project(response))
        except Exception as e:
            raise ValueError("Erro add project: " + str(e))

    def delete(self, project_name: str) -> str:
        url = self.__env.make_url(f"project/{project_name}")
        response = self.__http_adapter.delete(
            url=url, headers=self.__env.headers
        )
        self.refresh()
        return response

    def __str__(self) -> str:
        if len(self.__list_projects) == 0:
            return "No projects found."
        return str([x.name for x in self.__list_projects])

    def find_all(self) -> [str]:
        return [x.id for x in self.__list_projects]

    def find_by_name(self, name: str) -> [Project]:
        return [
            project for project in self.__list_projects if name in project.name
        ]

    def find_by_model(self, model_name: str) -> [Project]:
        return [
            project
            for project in self.__list_projects
            if model_name in project.model_name
        ]

    def find_by_created(
        self, start_date: datetime, end_date: datetime
    ) -> [Project]:
        return [
            project
            for project in self.__list_projects
            if start_date <= project.created_at <= end_date
        ]

    def find_by_last_used(
        self, start_date: datetime, end_date: datetime
    ) -> [Project]:
        return [
            project
            for project in self.__list_projects
            if start_date <= project.last_used <= end_date
        ]
