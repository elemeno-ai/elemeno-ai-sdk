import asyncio

import aiohttp
import pytest

from elemeno_ai_sdk.ml.inference.client import InferenceClient
from elemeno_ai_sdk.ml.inference.input_space import InputSpaceBuilder


ENDPOINT = "/v0/inference"


@pytest.fixture
def server(event_loop, aiohttp_server):
    async def handle_request(request):
        return aiohttp.web.Response(body='{"result": "cat"}', headers={"Content-Type": "application/json"})

    app = aiohttp.web.Application()
    app.router.add_post(ENDPOINT, handle_request)
    event_loop = asyncio.get_event_loop()
    return event_loop.run_until_complete(aiohttp_server(app, port=8080))


async def test_infer(server):
    input_space = InputSpaceBuilder().with_entity({"key": "cat"}).build()
    infer_client = InferenceClient("localhost", 8080, "http", ENDPOINT)
    result = await infer_client.infer(input_space)
    assert result["result"] == "cat"
