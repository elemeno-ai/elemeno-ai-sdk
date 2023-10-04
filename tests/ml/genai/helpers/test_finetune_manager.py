import sys

sys.path.append("./src")
sys.path.append("./tests")

from elemeno_ai_sdk.ml.genai.helpers.finetune_manager import FineTuneManager
from ml.genai.mocks.mock_environment import MockEnvironment
from ml.genai.mocks.mock_htt_adapter import MockHttpAdapter


ENV_HEADERS = {
    "resources": [
        {
            "name": "Project1",
            "model_name": "Model1",
            "id": "123",
        },
        {
            "name": "Project2",
            "model_name": "Model2",
            "id": "567",
        },
    ]
}

DATA = {"model": "model_name"}
FILE = "file_name"


class MockIFinetune:
    def get_body(self):
        return DATA

    def get_file(self):
        return FILE

    def get_endpoint(self):
        return "endpoint"


class TestFineTuneManager:
    def sut_make(self, env, http):
        return FineTuneManager(env, http)

    def test_finetune_fit(self):
        env = MockEnvironment()
        env.headers = ENV_HEADERS
        http = MockHttpAdapter()

        sut = self.sut_make(env, http)

        sut.fit("123", MockIFinetune())

        assert env.url == ["project/123/endpoint"]
        assert env.url == http.url
        assert http.body == DATA
        assert http.file == FILE
