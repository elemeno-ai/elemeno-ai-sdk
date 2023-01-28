
from elemeno_ai_sdk.cli import main
from .features.feature_store import *


def test_main():
    main([])

def test_no_broken_imports():
    from elemeno_ai_sdk.config import Configs
    assert(1==1)