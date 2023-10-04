import sys

sys.path.append("./src")
sys.path.append("./tests")

import pytest
from elemeno_ai_sdk.ml.genai.helpers.deploy_manager import DeployManager
from ml.genai.mocks.mock_environment import MockEnvironment
from ml.genai.mocks.mock_htt_adapter import MockHttpAdapter


ENV_HEADERS = {
    "resources": [
        {"name": "model_name1", "project_id": "Project1"},
        {"name": "model_name2", "project_id": "Project2"},
    ]
}


class MockPtWeight:
    path: str = None

    def __init__(self, value) -> None:
        self.path = value


class MockModel:
    ptweights = []

    def __init__(self, value) -> None:
        self.ptweights = [MockPtWeight(value)]


class MockDeploy:
    name: str
    project_id: str

    def __init__(self, value) -> None:
        self.name = value["name"]
        self.project_id = value["project_id"]


class MockModelManager:
    def get(self, value) -> MockModel:
        return MockModel(value)


class MockProjectManager:
    projects = []

    def find_all(self):
        return self.projects


class TestDeployManager:
    def sut_make(self, env, http, model, project):
        return DeployManager(env, http, model, project)

    def test_deploy_manager_refresh_deploys(self, mocker):
        env = MockEnvironment()
        env.headers = ENV_HEADERS
        http = MockHttpAdapter()
        model = MockModelManager()
        project = MockProjectManager()
        project.projects = ["Project1"]

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.deploy_manager.Deploy", MockDeploy
        )

        sut = self.sut_make(env, http, model, project)
        env.url = []
        http.url = []
        http.headers = []
        sut.refresh()

        assert env.url == [
            f"project/{project}/model-deploy?offset=0"
            for project in project.projects
        ]
        assert env.url == http.url
        assert str(sut) == str(
            [
                x["name"] + "-" + x["project_id"]
                for x in ENV_HEADERS["resources"]
            ]
        )

    def test_deploy_with_error(self, mocker):
        env = MockEnvironment()
        env.headers = ENV_HEADERS
        http = MockHttpAdapter()
        model = MockModelManager()
        project = MockProjectManager()
        project.projects = ["Project1"]

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.deploy_manager.Deploy", MockDeploy
        )

        sut = self.sut_make(env, http, model, project)
        env.raise_exception = True
        env.msg_exception = "Status Code 500"

        with pytest.raises(Exception) as e:
            sut.deploy("project_id_1", "deploy_name_1", "model_name_1")

        assert e.value.args[0] == "Erro add deploy: " + env.msg_exception

    def test_success_deploy(self, mocker):
        env = MockEnvironment()
        env.headers = ENV_HEADERS
        http = MockHttpAdapter()
        model = MockModelManager()
        project = MockProjectManager()
        project.projects = ["project_id_1"]

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.deploy_manager.Deploy", MockDeploy
        )

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.model_manager.ModelManager",
            MockModelManager,
        )

        sut = self.sut_make(env, http, model, project)
        env.url = []
        http.url = []
        http.headers = []
        sut.deploy("project_id_1", "deploy_name_1", "model_name_1")

        assert env.url == [
            f"project/{project}/model-deploy" for project in project.projects
        ]
        assert env.url == http.url
        assert http.body == {
            "name": "deploy_name_1",
            "checkpointPath": "model_name_1",
        }

    def test_find_by_name_return_None(self, mocker):
        env = MockEnvironment()
        env.headers = ENV_HEADERS
        http = MockHttpAdapter()
        model = MockModelManager()
        project = MockProjectManager()
        project.projects = ["project_id_1"]

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.deploy_manager.Deploy", MockDeploy
        )

        sut = self.sut_make(env, http, model, project)

        assert sut.find_by_name("deploy_name_3") is None

    def test_find_by_name_return_deploy(self, mocker):
        env = MockEnvironment()
        env.headers = ENV_HEADERS
        http = MockHttpAdapter()
        model = MockModelManager()
        project = MockProjectManager()
        project.projects = ["project_id_1"]

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.deploy_manager.Deploy", MockDeploy
        )

        sut = self.sut_make(env, http, model, project)

        assert sut.find_by_name("model_name1").name == "model_name1"
        assert sut.find_by_name("model_name1").project_id == "Project1"

    def test_find_by_project_return_None(self, mocker):
        env = MockEnvironment()
        env.headers = ENV_HEADERS
        http = MockHttpAdapter()
        model = MockModelManager()
        project = MockProjectManager()
        project.projects = ["project_id_1"]

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.deploy_manager.Deploy", MockDeploy
        )

        sut = self.sut_make(env, http, model, project)

        assert sut.find_by_project("deploy_name_3") is None

    def test_find_by_project_return_deploy(self, mocker):
        env = MockEnvironment()
        env.headers = ENV_HEADERS
        http = MockHttpAdapter()
        model = MockModelManager()
        project = MockProjectManager()
        project.projects = ["project_id_1"]

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.deploy_manager.Deploy", MockDeploy
        )

        sut = self.sut_make(env, http, model, project)

        assert sut.find_by_project("Project1").name == "model_name1"
        assert sut.find_by_project("Project1").project_id == "Project1"
