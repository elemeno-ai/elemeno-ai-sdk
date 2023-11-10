import os
from functools import wraps
from typing import Callable, Optional

import aiohttp

from elemeno_ai_sdk.logger import logger


def mlhub_auth(func: Callable):
    @wraps(func)
    async def wrapper(
        self,
        *args,
        session: Optional[aiohttp.ClientSession] = None, 
        **kwargs):

        if session is None:
            api_key = os.getenv("MLHUB_API_KEY")
            if api_key is None:
                raise ValueError("Please set the MLHUB_API_KEY environment variable.")
            headers = {
                "x-api-key": api_key, 
                "authorization": f"Bearer {api_key}",
                }
            async with aiohttp.ClientSession(headers=headers) as session:
                kwargs["session"] = session
                return await func(self, *args, **kwargs)
        else:
            logger.warning(
                "Calling a method anottated with with_auth and passing a session object is not recommended in production."
            )
            kwargs["session"] = session
            return await func(self, *args, **kwargs)

    return wrapper
