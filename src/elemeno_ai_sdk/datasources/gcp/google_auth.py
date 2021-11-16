import elemeno_ai_sdk
from elemeno_ai_sdk.config import Configs

class Authenticator:

    def __init__(self):
        self._config = Configs.instance()

    @property
    def config(self):
        return self._config
    
    @config.setter
    def set_config(self, config):
        self._config = config

    def get_credentials(self):
        mode = self.config.app.mode
        auth = None
        if mode == "production":
            auth = elemeno_ai_sdk.datasources.gcp.sa.authenticator.Authenticator()
        else:
            auth = elemeno_ai_sdk.datasources.gcp.appflow.authenticator.Authenticator()
        return auth.authenticate()

