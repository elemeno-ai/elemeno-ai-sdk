import sys

sys.path.append("./src")


import pytest
import requests

import elemeno_ai_sdk.ml.genai.protocols.http_adapter
from elemeno_ai_sdk.ml.genai.protocols.http_adapter import HttpAdapter


class MockRequests:
    status_code: int = 201

    def get(self, url, headers):
        self.url = url
        self.headers = headers

        return self

    def post(self, url, json=None, headers=None, files=None):
        self.url = url
        self.headers = headers
        self.ijson = json
        self.files = files

        return self

    def delete(self, url, headers):
        self.url = url
        self.headers = headers

        return self

    def json(self):
        return {"Json": "Json"}


class TestHttpAdapter:
    def make_sut(self):
        return HttpAdapter()

    def test_raise_exception_http_get(self, mocker):
        mocker.patch(
            "requests.get",
            side_effect=requests.exceptions.RequestException("Error"),
        )
        sut = self.make_sut()

        with pytest.raises(Exception) as e:
            sut.get("url", {})

        assert e.value.args[0] == "HTTP Get request failed: Error"

    def test_get_reponse_status_code_diff_201(self, mocker):
        mock_requests = MockRequests()
        mock_requests.status_code = 400
        sut = self.make_sut()
        mocker.patch.object(
            elemeno_ai_sdk.ml.genai.protocols.http_adapter,
            "requests",
            new=mock_requests,
        )

        with pytest.raises(ValueError) as e:
            sut.get("url", {})

        print(e.value.args[0])
        assert e.value.args[0] == "HTTP Get request failed Status Code: 400"
        assert mock_requests.url == "url"
        assert mock_requests.headers == {}

    def test_get_reponse_status_code_200(self, mocker):
        mock_requests = MockRequests()
        mock_requests.status_code = 200
        sut = self.make_sut()
        mocker.patch.object(
            elemeno_ai_sdk.ml.genai.protocols.http_adapter,
            "requests",
            new=mock_requests,
        )

        assert sut.get("url", {"headers": "headers"}) == {"Json": "Json"}
        assert mock_requests.url == "url"
        assert mock_requests.headers == {"headers": "headers"}

    def test_exception_http_post(self, mocker):
        mocker.patch(
            "requests.post",
            side_effect=requests.exceptions.RequestException("Error"),
        )
        sut = self.make_sut()

        with pytest.raises(Exception) as e:
            sut.post("url", {"headers": "headers"}, {"json": "body"})

        assert e.value.args[0] == "HTTP Post request failed: Error"

    def test_post_reponse_status_code_diff_201(self, mocker):
        mock_requests = MockRequests()
        mock_requests.status_code = 400
        sut = self.make_sut()
        mocker.patch.object(
            elemeno_ai_sdk.ml.genai.protocols.http_adapter,
            "requests",
            new=mock_requests,
            create=False,
        )

        with pytest.raises(ValueError) as e:
            sut.post("url", {"headers": "headers"}, {"json": "body"})

        assert e.value.args[0] == "HTTP Post request failed Status Code: 400"
        assert mock_requests.url == "url"
        assert mock_requests.headers == {"headers": "headers"}
        assert mock_requests.ijson == {"json": "body"}

    def test_post_reponse_status_code_201_without_files(self, mocker):
        mock_requests = MockRequests()
        sut = self.make_sut()
        mocker.patch.object(
            elemeno_ai_sdk.ml.genai.protocols.http_adapter,
            "requests",
            new=mock_requests,
            create=False,
        )

        assert sut.post("url", {"headers": "headers"}, {"json": "body"}) == {
            "Json": "Json"
        }
        assert mock_requests.url == "url"
        assert mock_requests.headers == {"headers": "headers"}
        assert mock_requests.ijson == {"json": "body"}
        assert not mock_requests.files

    def test_post_reponse_status_code_201_with_files(self, mocker):
        mock_requests = MockRequests()
        sut = self.make_sut()
        mocker.patch.object(
            elemeno_ai_sdk.ml.genai.protocols.http_adapter,
            "requests",
            new=mock_requests,
            create=False,
        )

        assert sut.post(
            "url", {"headers": "headers"}, {"json": "body"}, file="path/file"
        ) == {"Json": "Json"}
        assert mock_requests.url == "url"
        assert mock_requests.headers == {"headers": "headers"}
        assert mock_requests.ijson == {"json": "body"}
        assert mock_requests.files == "path/file"

    def test_exception_http_delete(self, mocker):
        mocker.patch(
            "requests.delete",
            side_effect=requests.exceptions.RequestException("Error"),
        )
        sut = self.make_sut()

        with pytest.raises(Exception) as e:
            sut.delete("url", {"headers": "headers"})

        assert e.value.args[0] == "HTTP Delete request failed: Error"

    def test_delete_reponse_status_code_diff_201(self, mocker):
        mock_requests = MockRequests()
        mock_requests.status_code = 400
        sut = self.make_sut()
        mocker.patch.object(
            elemeno_ai_sdk.ml.genai.protocols.http_adapter,
            "requests",
            new=mock_requests,
            create=False,
        )

        with pytest.raises(ValueError) as e:
            sut.delete("url", {"headers": "headers"})

        assert e.value.args[0] == "HTTP Delete request failed Status Code: 400"
        assert mock_requests.url == "url"
        assert mock_requests.headers == {"headers": "headers"}

    def test_delete_reponse_status_code_201(self, mocker):
        mock_requests = MockRequests()
        sut = self.make_sut()
        mocker.patch.object(
            elemeno_ai_sdk.ml.genai.protocols.http_adapter,
            "requests",
            new=mock_requests,
            create=False,
        )

        assert sut.delete("url", {"headers": "headers"}) == {"Json": "Json"}
        assert mock_requests.url == "url"
        assert mock_requests.headers == {"headers": "headers"}
