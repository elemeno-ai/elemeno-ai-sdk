import json
import logging
import os
from typing import Iterable, Mapping

import requests
from omegaconf import OmegaConf


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
        cfg_path = os.getenv("ELEMENO_CFG_FILE", "elemeno.yaml")
        try:
            # check if cfg_path exists
            if not os.path.exists(cfg_path):
                logging.warning("Couldn't find a config file at %s, will continue without loading it", cfg_path)
                props = OmegaConf.create()
                return props
            if cls._instance is None or force_reload:
                cls._instance = cls.__new__(cls)
                cls._props = OmegaConf.load(cfg_path)
            return cls._instance.props
        except Exception as e:
            logging.error("Unexpected error when instantiating the config object", e)
            props = OmegaConf.create()
            return props

    @property
    def props(self):
        return self._props

    def auth_fs(self):
        JWT_TOKEN_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jwt_token")
        SAAS_ADRESS = "https://c3po-stg.elemeno.ai/"

        with open(JWT_TOKEN_PATH) as f:
            JWT_TOKEN = json.load(f)

        headers = {"x-token: Bearer {}".format(JWT_TOKEN)}

        response = requests.get(SAAS_ADRESS + "user/settings/sdk/authentication", headers=headers)
        # globals().update(response)

        return json.loads(response)

    def parse_jwt_token(self, jwt_token: Mapping[str, Iterable[str]]):
        pass
