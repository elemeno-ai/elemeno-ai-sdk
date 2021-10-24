from omegaconf import OmegaConf

class Configs:
    _instance = None
    _props = None

    def __init__(self):
        raise RuntimeError("Call instance() instead")

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = cls.__new__(cls)
            cls._props = OmegaConf.load('elemeno.yaml')
        return cls._instance.props

    @property
    def props(self):
        return self._props
