from typing import List
from google_auth_oauthlib import flow
from elemeno_ai_sdk.datasources.base_authenticator import BaseAuthenticator
from elemeno_ai_sdk.config import Configs


config = Configs.instance()

BaseAuthenticator.register
class Authenticator:
    def authenticate(self, file_path: str = None, scopes: List[str] = None):
        if not file_path:
            # load file path from the config file
            file_path = config.gcp.appflow.client_secret.file
        if not scopes:
            scopes = config.gcp.appflow.scopes
        appflow = flow.InstalledAppFlow.from_client_secrets_file(client_secrets_file=file_path, scopes=list(scopes))
        appflow.run_console()
        return appflow.credentials

