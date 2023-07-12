import logging
import os
from omegaconf import OmegaConf
import json
import requests
from typing import Mapping, Iterable

class Configs:
    """
    Loads the configuration from the elemeno.yaml file. This class looks for the file in the current directory,
    unless specified with the environment variable ELEMENO_CFG_FILE.

    To be able to use environment variables within elemeno.yaml, you need to follow omegaconf specifications. For example:
    ```
    feature_store:
      feast_config_path: ${oc.env:FEAST_CONFIG_PATH}
    ```
    """
    _instance = None
    _props = OmegaConf.create()

    def __init__(self):
      raise RuntimeError("Call instance() instead")

    @classmethod
    def parse(cls, configdict):
        if cls._props:
          cls._props = OmegaConf.merge(cls._props, OmegaConf.create(configdict))
        else:
          cls._props = OmegaConf.create(configdict)
        cls._instance = cls.__new__(cls)
        return cls._instance.props

    @classmethod
    def instance(cls, force_reload=False):
      try:
        if cls._instance is None or force_reload:
          cls._instance = cls.__new__(cls)
          remote_config = cls.retrieve_remote_config()
          logging.info("REMOVE ME: remote_config: %s", remote_config)
          cls._props = OmegaConf.create(obj=remote_config)
        return cls._instance.props
      except Exception as e:
        if "retrieving remote config" in e.args[0]:
          raise e
        logging.error("Unexpected error when instantiating the config object", e)
        cls._instance = None
        props = OmegaConf.create()
        return props

    @property
    def props(self):
      return self._props
    

    def retrieve_remote_config():
      try:
        SEMANTIX_API_KEY = os.getenv('SEMANTIX_API_KEY')
        if SEMANTIX_API_KEY is None:
          raise ValueError("SEMANTIX_API_KEY environment variable is not set")
        
        SAAS_ADRESS = os.getenv("SEMANTIX_API_BASE", 'https://c3po-stg.elemeno.ai/')

        headers = {
          'x-api-secret': 'true',
          'x-api-key': "{}".format(SEMANTIX_API_KEY)
          }

        response = requests.get(SAAS_ADRESS + 'user/settings/sdk/credential', headers=headers)
        
        if not str(response.status_code).startswith('2'):
          raise ValueError("Error ({}) retrieving remote config: {}".format(response.status_code,response.text))
        
        print(response)
        return json.loads(response.data)
      except Exception as e:
        logging.error("Unexpected error when retrieving remote config", e)
        raise e
    
    def parse_jwt_token(self, jwt_token: Mapping[str, Iterable[str]]):
        pass