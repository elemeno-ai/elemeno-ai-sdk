import sys

sys.path.append("./src")
sys.path.append("./tests")

import pytest
from elemeno_ai_sdk.ml.genai.helpers.model_manager import ModelManager
from ml.genai.mocks.mock_environment import MockEnvironment
from ml.genai.mocks.mock_htt_adapter import MockHttpAdapter


class MockModel:
    model_name: str

    def __init__(self, value) -> None:
        self.model_name = value["model_name"]


class TestModelManager:
    def sut_make(self, env, htp) -> ModelManager:
        return ModelManager(env, htp)

    def test_model_name_not_found(self, mocker):
        env = MockEnvironment()
        env.headers = {
            "resources": [
                {"model_name": "model_name1"},
                {"model_name": "model_name2"},
            ]
        }
        http = MockHttpAdapter()

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.model_manager.Model", MockModel
        )

        model_manager = self.sut_make(env, http)
        model_name = "ModelNotFound"

        pytest

        with pytest.raises(Exception) as e:
            model_manager.get(model_name)

        assert e.value.args[0] == f"Model {model_name} not found."

    def test_model_retunr_correct_model(self, mocker):
        env = MockEnvironment()
        env.headers = {
            "resources": [
                {"model_name": "model_name1"},
                {"model_name": "model_name2"},
            ]
        }
        http = MockHttpAdapter()

        mocker.patch(
            "elemeno_ai_sdk.ml.genai.helpers.model_manager.Model", MockModel
        )

        model_manager = self.sut_make(env, http)
        model_name = "model_name1"

        model = model_manager.get(model_name)

        assert model.model_name == model_name
        assert http.url == env.url
        assert http.headers[0] == env.headers
