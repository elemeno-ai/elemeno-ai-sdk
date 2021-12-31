import os
import torch
import mlflow
import mlflow.pyfunc
import onnx
from elemeno_ai_sdk.config import Configs

class ModelRegistry:

    def __init__(self):
        cfg = Configs.instance()
        self._config = cfg
        os.environ["AWS_ACCESS_KEY_ID"] = cfg.cos.key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = cfg.cos.secret
        #TODO fix this to load from config file
        os.environ["MLFLOW_S3_IGNORE_TLS"] = "true"
        minio_host = cfg.cos.host
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = f"http://{minio_host}"
        mlflow.set_tracking_uri(cfg.registry.tracking_url)


    def get_latest_model_torch(self, model_name: str, device: str):
        """Loads the most recent model registered and in stage Production
        from the model registry. Returns an mlflow model object.

        Keyword arguments:
        model_name -- the name of the model in the registry
        """
        stage = 'Production'

        model = mlflow.pytorch.load_model(
            model_uri=f"models:/{model_name}/{stage}",
            map_location=torch.device(device)
        )

        return model

    def get_latest_model_onnx(self, model_name: str):
        """Loads the most recent model registered and in stage Production
        from the model registry. Returns an onnx model object.

        Keyword arguments:
        model_name -- the name of the model in the registry
        """
        stage = 'Production'

        model = mlflow.onnx.load_model(
            model_uri=f"models:/{model_name}/{stage}"
        )
        # saves the file to disk
        onnx.save(model, 'model.onnx')

        return model

    def get_latest_model(self, model_name: str):
        """Loads the most recent model registered and in stage Production
        from the model registry. Returns an mlflow model object.

        Keyword arguments:
        model_name -- the name of the model in the registry
        """
        stage = 'Production'

        model = mlflow.pyfunc.load_model(
            model_uri=f"models:/{model_name}/{stage}",
        )

        return model
