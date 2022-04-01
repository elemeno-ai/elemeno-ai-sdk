import logging
from omegaconf import OmegaConf
import os

class Configs:
    _instance = None
    _props = None

    def __init__(self):
        raise RuntimeError("Call instance() instead")

    @classmethod
    def instance(cls):
        cfg_path = os.getenv('ELEMENO_CFG_PATH', 'elemeno.yaml')
        try:
            if cls._instance is None:
                cls._instance = cls.__new__(cls)
                cls._props = OmegaConf.load(cfg_path)
            return cls._instance.props
        except:
            logging.info("Couldn't find a config file at %s, will continue without loading it", cfg_path)
            return None

    @property
    def props(self):
        return self._props
