import sys

sys.path.append("./src")
sys.path.append("./tests")

from datetime import date, timedelta
import pytest
from elemeno_ai_sdk.ml.genai.helpers.project_manager import ProjectManager
from ml.genai.mocks.mock_environment import MockEnvironment
from ml.genai.mocks.mock_htt_adapter import MockHttpAdapter


ENV_HEADERS = {
    "resources": [
        {
            "name": "Project1",
            "model_name": "Model1",
            "id": "123",
            "created_at": date.today() - timedelta(5),
            "last_used": date.today() - timedelta(5),
        },
        {
            "name": "Project2",
            "model_name": "Model2",
            "id": "567",
            "created_at": date.today() - timedelta(10),
            "last_used": date.today() - timedelta(5),
        },
    ]
}


class MockModel:
    id: str

    def __init__(self, value) -> None:
        self.id = value


class MockModelManager:
    def get(self, value) -> MockModel:
        return MockModel(value)


class MockProject:
    name: str
    model_name: str
    id: str
    created_at: date
    last_used: date

    def __init__(self, value) -> None:
        self.name = value["name"]
        self.model_name = value["model_name"]
        self.id = value["id"]
        self.created_at = value["created_at"]
        self.last_used = value["last_used"]


class TestProjectManager:
    def sut_make(self, env, http, model):
        return ProjectManager(env, http, model)

    def test_project_manager_refresh_projects(self, mocker):
        env = MockEnvironment()
        env.headers = ENV_HEADERS
        http = MockHttpAdapter()
        model = MockModelManager()

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.project_manager.Project",
            MockProject,
        )

        sut = self.sut_make(env, http, model)
        env.url = []
        http.url = []
        http.headers = []
        sut.refresh()

        assert env.url == ["project"]
        assert env.url == http.url
        assert str(sut) == str(["Project1", "Project2"])

    def test_create_project_with_error(self, mocker):
        env = MockEnvironment()
        env.headers = ENV_HEADERS
        http = MockHttpAdapter()
        model = MockModelManager()

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.project_manager.Project",
            MockProject,
        )

        sut = self.sut_make(env, http, model)
        env.raise_exception = True
        env.msg_exception = "Status Code 500"

        with pytest.raises(Exception) as e:
            sut.create("project_name_1", "model_name_1")

        assert e.value.args[0] == "Erro add project: " + env.msg_exception

    def test_success_create_project_without_secrets(self, mocker):
        env = MockEnvironment()
        env.headers = {
            "name": "Project1",
            "model_name": "Model1",
            "id": "123",
            "created_at": "date",
            "last_used": "date",
        }
        http = MockHttpAdapter()
        model = MockModelManager()

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.project_manager.Project",
            MockProject,
        )

        sut = self.sut_make(env, http, model)
        env.url = []
        http.url = []
        http.headers = []
        sut.create("project_name_1", "model_name_1")

        assert env.url == ["project"]
        assert env.url == http.url
        assert http.body == {
            "modelId": "model_name_1",
            "name": "project_name_1",
        }
        assert str(sut) == str(["Project1"])

    def test_success_create_project_with_secrets(self, mocker):
        env = MockEnvironment()
        env.headers = {
            "name": "Project1",
            "model_name": "Model1",
            "id": "123",
            "created_at": "date",
            "last_used": "date",
        }
        secrets = {"key": "value"}
        http = MockHttpAdapter()
        model = MockModelManager()

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.project_manager.Project",
            MockProject,
        )

        sut = self.sut_make(env, http, model)
        env.url = []
        http.url = []
        http.headers = []
        sut.create("project_name_1", "model_name_1", secrets=secrets)

        assert env.url == ["project"]
        assert env.url == http.url
        assert http.body == {
            "modelId": "model_name_1",
            "name": "project_name_1",
            "secrets": secrets,
        }
        assert str(sut) == str(["Project1"])

    def test_delete_project(self):
        env = MockEnvironment()
        http = MockHttpAdapter()
        model = MockModelManager()

        sut = self.sut_make(env, http, model)
        env.url = []
        http.url = []
        http.headers = []

        sut.delete("Project1")

        assert env.url == ["project/Project1", "project"]
        assert env.url == http.url

    def test_project_manager_find_all(self, mocker):
        env = MockEnvironment()
        env.headers = ENV_HEADERS
        http = MockHttpAdapter()
        model = MockModelManager()

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.project_manager.Project",
            MockProject,
        )

        sut = self.sut_make(env, http, model)
        result = sut.find_all()

        assert result == ["123", "567"]

    def test_project_manager_find_by_name(self, mocker):
        env = MockEnvironment()
        env.headers = ENV_HEADERS
        http = MockHttpAdapter()
        model = MockModelManager()

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.project_manager.Project",
            MockProject,
        )

        sut = self.sut_make(env, http, model)
        print(sut)
        result = sut.find_by_name("1")

        assert [project.id for project in result] == ["123"]

    def test_project_manager_find_by_model(self, mocker):
        env = MockEnvironment()
        env.headers = ENV_HEADERS
        http = MockHttpAdapter()
        model = MockModelManager()

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.project_manager.Project",
            MockProject,
        )

        sut = self.sut_make(env, http, model)
        print(sut)
        result = sut.find_by_name("2")

        assert [project.id for project in result] == ["567"]

    def test_project_manager_find_by_created(self, mocker):
        env = MockEnvironment()
        env.headers = ENV_HEADERS
        http = MockHttpAdapter()
        model = MockModelManager()

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.project_manager.Project",
            MockProject,
        )

        sut = self.sut_make(env, http, model)
        print(sut)
        result = sut.find_by_created(
            date.today() - timedelta(6), date.today() - timedelta(2)
        )

        assert [project.id for project in result] == ["123"]

    def test_project_manager_find_by_last_used(self, mocker):
        env = MockEnvironment()
        env.headers = ENV_HEADERS
        http = MockHttpAdapter()
        model = MockModelManager()

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.project_manager.Project",
            MockProject,
        )

        sut = self.sut_make(env, http, model)
        print(sut)
        result = sut.find_by_created(
            date.today() - timedelta(6), date.today() - timedelta(2)
        )

        assert [project.id for project in result] == ["123"]
