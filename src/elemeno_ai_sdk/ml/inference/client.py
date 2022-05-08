import asyncio
from asyncio.log import logger
from typing import Optional
import aiohttp
from elemeno_ai_sdk.ml.inference.input_space import InputSpace, InputSpaceBuilder

class InferenceClient:

    def __init__(self, host, port=443, protocol='https', 
        endpoint="/v0/inference", session: Optional[aiohttp.ClientSession] = None):
      self.host = host
      self.port = port
      self.protocol = protocol
      self.endpoint = endpoint
      self.base_url = f'{protocol}://{host}:{port}'
      self.session = session

    async def infer(self, input_data: InputSpace):
      try:
        session = aiohttp.ClientSession(self.base_url, headers={'Content-Type': 'application/json'}) if self.session is None else self.session
        async with session.post(f'{self.endpoint}', json=input_data.entities) as response:
          try:
            return await response.json()
          except Exception as e:
            print("error:", e)
            return await response.text()
      except:
        logger.error("Error connecting to server")
      finally:
        await session.close()