import logging
import os
from omegaconf import OmegaConf

class Configs:
    """ Loads the configuration from the elemeno.yaml file. This class looks for the file in the current directory,
    unless specified with the environment variable ELEMENO_CFG_FILE.

    To be able to use environment variables within elemeno.yaml, you need to follow omegaconf specifications. For example:
    ```
    feature_store:
      feast_config_path: ${oc.env:FEAST_CONFIG_PATH}
    ```
    """
    _instance = None
    _props = None

    def __init__(self):
        raise RuntimeError("Call instance() instead")

    @classmethod
    def instance(cls, force_reload=False):
        cfg_path = os.getenv('ELEMENO_CFG_FILE', 'elemeno.yaml')
        try:
            if cls._instance is None or force_reload:
                cls._instance = cls.__new__(cls)
                cls._props = OmegaConf.load(cfg_path)
            return cls._instance.props
        except:
            logging.warning("Couldn't find a config file at %s, will continue without loading it", cfg_path)
            return None

    @property
    def props(self):
        return self._props
