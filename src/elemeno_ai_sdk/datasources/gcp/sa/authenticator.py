from google.oauth2 import service_account
from elemeno_ai_sdk.datasources.base_authenticator import BaseAuthenticator
from elemeno_ai_sdk.config import Configs


config = Configs.instance()

BaseAuthenticator.register
class Authenticator:
    def authenticate(self, file_path: str = None):
        if not file_path:
            # load file path from the config file
            file_path = config.gcp.sa.file
        return service_account.Credentials.from_service_account_file(file_path)

