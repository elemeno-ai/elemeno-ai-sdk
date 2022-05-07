from typing import Optional
import aiohttp
from elemeno_ai_sdk.ml.inference.input_space import InputSpace

class InferenceClient:

    def __init__(self, host, port=443, protocol='https', 
        endpoint="/v0/inference", session: Optional[aiohttp.ClientSession] = None):
      self.host = host
      self.port = port
      self.protocol = protocol
      self.endpoint = endpoint
      base_url = f'{protocol}://{host}:{port}'
      self.session = session if session != None else aiohttp.ClientSession(base_url, headers={'Content-Type': 'application/json'})

    async def infer(self, input_data: InputSpace):
      async with self.session.post(f'{self.endpoint}', json=input_data.entities) as response:
        return await response.json()
