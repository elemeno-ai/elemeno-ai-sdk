import mlflow
import mlflow.pyfunc
from elemeno_ai_sdk.config import Configs

class ModelRegistry:

    def __init__(self, mlflow_tracking_url: str):
        cfg = Configs.instance()
        self._config = cfg
        mlflow.set_tracking_uri(cfg.registry.tracking_url)

    def get_latest_model(self, model_name: str):
        """Loads the most recent model registered and in stage Production
        from the model registry. Returns an mlflow model object.

        Keyword arguments:
        model_name -- the name of the model in the registry
        """
        stage = 'Production'

        model = mlflow.pyfunc.load_model(
            model_uri=f"models:/{model_name}/{stage}"
        )

        return model
