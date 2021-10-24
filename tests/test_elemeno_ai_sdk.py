
from elemeno_ai_sdk.cli import main


def test_main():
    main([])

def test_no_broken_imports():
    from elemeno_ai_sdk.config import Configs
    from elemeno_ai_sdk.datasources.gcp import google_auth
    from elemeno_ai_sdk.datasources.gcp.sa.authenticator import Authenticator
    from elemeno_ai_sdk.datasources.gcp.appflow.authenticator import Authenticator
    assert(1==1)
