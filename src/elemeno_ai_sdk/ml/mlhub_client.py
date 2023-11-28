import json
import os
from typing import Any, Dict, Optional

import aiohttp
from tenacity import AsyncRetrying, RetryError, stop_after_attempt, wait_fixed

from elemeno_ai_sdk.logger import logger
from elemeno_ai_sdk.utils import mlhub_auth


# MLHub urls
PROD_URL = "https://c3po.ml.semantixhub.com"
DEV_URL = "https://c3po-stg.ml.semantixhub.com"

# Retry params
STOP_AFTER_ATTEMPT = 5
WAIT_FIXED = 1


class MLHubRemote:
    def __init__(self, env: Optional[str] = None):
        if env:
            self._env = env

    @property
    def env(self):
        return self._env if hasattr(self, "_env") else os.getenv("MLHUB_ENV", "prod")

    @env.setter
    def set_env(self, env: str):
        self._env = env

    @property
    def base_url(self):
        base_url = None
        if self._env == "prod":
            base_url = PROD_URL
        elif self._env == "dev":
            base_url = DEV_URL
        else:
            raise ValueError("Invalid environment. Please use dev or prod.")
        return base_url

    @mlhub_auth
    async def post(
        self,
        url: str,
        body: Dict[str, Any] = None,
        session: Optional[aiohttp.ClientSession] = None,
        file=None,
    ):
        if file is not None and body is not None:
            raise ValueError("Either body or file can be sent, but not both.")
        elif file is not None:
            data = file
        else:
            data = json.dumps(body)

        try:
            retry_operator = AsyncRetrying(stop=stop_after_attempt(STOP_AFTER_ATTEMPT), wait=wait_fixed(WAIT_FIXED))
            async for attempt in retry_operator:
                with attempt:
                    async with session.post(url=url, data=data) as response:
                        if not response.ok:
                            raise ValueError(
                                f"Failed post to {url} with: \n"
                                f"\t status code= {response.status} \n"
                                f"\t message_body= {body} \n"
                                f"\t header= {session.headers}"
                            )
                        return await response.text()
        except RetryError:
            logger.exception("Max retries reached")
            return None

    @mlhub_auth
    async def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        is_binary: bool = False,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        try:
            retry_operator = AsyncRetrying(stop=stop_after_attempt(STOP_AFTER_ATTEMPT), wait=wait_fixed(WAIT_FIXED))
            async for attempt in retry_operator:
                with attempt:
                    async with session.get(url=url, params=params) as response:
                        if not response.ok:
                            raise ValueError(
                                f"Failed to get from {url} with: \n"
                                f"\t params= {params} \n"
                                f"\t status code= {response.status} \n"
                                f"\t header= {session.headers}"
                            )

                        if is_binary:
                            return await response.content.read()

                        return await response.json(content_type=response.content_type)
        except RetryError:
            logger.exception("Max retries reached")
            return None
