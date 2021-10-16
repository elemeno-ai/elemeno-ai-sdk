import abc
import elemeno_ai_sdk
from elemeno_ai_sdk.config import Configs

class BaseAuthenticator(metaclass=abc.ABCMeta):
    
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'authenticate') and
                callable(subclass.authenticate))

config = Configs().props
class Authenticator:

    def get_credentials(self):
        mode = config.app.mode
        auth = None
        if mode == "production":
            auth = elemeno_ai_sdk.datasources.gcp.sa.authenticator.Authenticator()
        else:
            auth = elemeno_ai_sdk.datasources.gcp.appflow.authenticator.Authenticator()
        return auth.authenticate()

