import sys
import os

sys.path.append("./src")

import pytest

from elemeno_ai_sdk.ml.genai.entities.generics import PROD_URL, DEV_URL
from elemeno_ai_sdk.ml.genai.protocols.environment import Environment


class TestEnviroments:
    def make_sut(self, env: str):
        return Environment(env)

    def test_valid_url_prod(self):
        os.environ["API_KEY"] = "Xavbdee"
        sut = self.make_sut("prod")
        endpoint = "test_prod"

        url = sut.make_url(endpoint)

        assert url == f"{PROD_URL}/generative/{endpoint}"

    def test_valid_url_dev(self):
        os.environ["API_KEY"] = "Xavbdee"
        sut = self.make_sut("dev")
        endpoint = "test_dev"

        url = sut.make_url(endpoint)

        assert url == f"{DEV_URL}/generative/{endpoint}"

    def test_valid_setting_api_key(self):
        os.environ["API_KEY"] = "Xavbdee"
        sut = self.make_sut("dev")

        assert sut.headers == {"x-api-key": os.getenv("API_KEY")}

    def test_exception_incorrect_env(self):
        os.environ["API_KEY"] = "Xavbdee"
        with pytest.raises(ValueError) as exception:
            sut = self.make_sut("test")

        assert (
            exception.value.args[0]
            == "Invalid environment. Please use dev or prod."
        )

    def test_environment_api_key_missin(self):
        os.environ["API_KEY"] = ""
        with pytest.raises(ValueError) as exception:
            sut = self.make_sut("prod")

        assert (
            exception.value.args[0]
            == "API_KEY environment variable not found or empty."
        )
