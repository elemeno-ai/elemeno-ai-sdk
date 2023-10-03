import asyncio
from typing import Optional

import aiohttp
from omegaconf import OmegaConf

from elemeno_ai_sdk import logger
from elemeno_ai_sdk.ml.mlhub_client import MLHubRemote
from elemeno_ai_sdk.utils import mlhub_auth


class Configs:
    def __init__(self, env: Optional[str] = None):
        if env:
            self._env = env

    @mlhub_auth
    async def _retrieve_remote_config(self, session: aiohttp.ClientSession = None):
        env = self._env if hasattr(self, "_env") else None
        base_url = MLHubRemote(env=env).base_url
        async with session.get(url=f"{base_url}/user/settings/sdk/authentication") as response:
            if not response.ok:
                logger.exception(f"Failed to retrieve config from remote with: \n" f"\t status code= {response.status}")
            return await response.json()

    def load_config(self, config: Optional[dict] = None):
        if config is not None:
            logger.debug("Using provided config via code, will not load remote")
        else:
            config = asyncio.run(self._retrieve_remote_config())
        return OmegaConf.create(config)
