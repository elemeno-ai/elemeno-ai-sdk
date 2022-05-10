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

if __name__ == '__main__':
  cli = InferenceClient('infserv-openaitest.app.elemeno.ai', 443, 'https', '/v0/inference')
  print("ok")
  input_header = """\"\"\"Publicação de uma empresa para comunicar com profissionais gestores de TI. Usar voz ativa.
Titulo: Gestão do conhecimento: princípios e benefícios.
Persona: Gestor de TI.
Objetivo: Abordar os princípios da gestão do conhecimento (vide tópicos). Ao falar de educação corporativa, falar sobre construir uma cultura de aprendizado e desenvolvimento contínuo. Abordar também como estruturar um sistema de gestão do conhecimento e os benefícios, trazendo bastante o foco em resultado prático, números, retorno da gestão do conhecimento e Business Agility.
\"\"\""""
  input = InputSpaceBuilder().with_entities({'prompt': [input_header + """

1. Inteligência competitiva
2. Gestão de competências
3. Gestão do capital intelectual
4. Gestão da informação
5. Educação corporativa

Inteligência competitiva:
""".strip()]}).build()

  num_results = 0
  target_num_results = 100
  eventloop = asyncio.get_event_loop()
  complete_result = ""
  while num_results < target_num_results:
    print("Calling inference...")
    if len(complete_result) > 0:
      start_str = min(len(complete_result), 200)
      input.entities = {"prompt": [input_header + complete_result[-start_str:]]}
    print(input.entities)
    result = eventloop.run_until_complete(cli.infer(input))
    complete_result += result["choices"][0]
    num_results += len(complete_result.split(" "))
    print(num_results)
  print(complete_result)
  ## Ferramentas de RH: tendências em gestão de pessoas ## Ferramenta de pesquisa de clima ## Ferramenta de assessment ## Ferramenta de gestão de pessoas ## LMS e LXS ## Análise de dados # Benefícios da tecnologia no RH # Por que investir em um RH Estratégico" Perguntas a serem respondidas: "Quais ferramentas de tecnologia são usadas no RH? Quais os impactos da tecnologia na área de recursos humanos? Como a tecnologia pode ajudar os departamentos de RH?