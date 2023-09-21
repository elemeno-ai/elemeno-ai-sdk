import os
import aiohttp
import logging
from typing import Callable, Optional
from functools import wraps

PROD_URL = "https://c3po.ml.semantixhub.com"
DEV_URL = "https://c3po-stg.ml.semantixhub.com"

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
        if self.env == "prod":
            base_url = PROD_URL
        elif self.env == "dev":
            base_url = DEV_URL
        else:
            raise ValueError("Invalid environment. Please use dev or prod.")
        return base_url

def mlhub_auth(func: Callable):
    @wraps(func)
    async def wrapper(self, *args, session: aiohttp.ClientSession = None, **kwargs):
        if session is None:
            api_key = os.getenv("MLHUB_API_KEY")
            if api_key is None:
                raise ValueError("Please set the MLHUB_API_KEY environment variable.")
            headers = {"Content-Type": "application/json", "x-api-key": api_key, "authorization": f"Bearer {api_key}"}
            async with aiohttp.ClientSession(headers=headers) as session:
                kwargs["session"] = session
                return await func(self, *args, **kwargs)
        else:
            logging.warning("Calling a method anottated with with_auth and passing a session object is not recommended in production.")
            kwargs["session"] = session
            return await func(self, *args, **kwargs)
    return wrapper