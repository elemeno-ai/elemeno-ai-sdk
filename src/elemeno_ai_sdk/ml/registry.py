import os
import torch
import mlflow  # type: ignore
import mlflow.pyfunc
import onnx
from elemeno_ai_sdk.config import Configs
from typing import Dict, List, Optional


class ModelRegistry:

    def __init__(self):
        cfg = Configs.instance()
        self._config = cfg
        os.environ["AWS_ACCESS_KEY_ID"] = cfg.cos.key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = cfg.cos.secret
        os.environ["MLFLOW_S3_IGNORE_TLS"] = cfg.registry.ignore_tsl
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = cfg.registry.endpoint_url
        mlflow.set_tracking_uri(cfg.registry.tracking_url)
        self.client = mlflow.MlflowClient()

    def tag_model(self, model_name: str, tags: Dict[str, str]) -> None:
        for tag_name, tag_value in tags.items():
            self.client.set_registered_model_tag(name=model_name, key=tag_name, value=tag_value)

    def save_model(self, model_file: str, model_name: str, tags: Optional[Dict[str, str]] = None) -> None:
        with mlflow.start_run(tags=tags):
            mlflow.sklearn.log_model()
            mlflow.pyfunc.log_model(artifact_path=model_file, registered_model_name=model_name)

        if tags is not None:
            self.tag_model(model_name=model_name, tags=tags)

    def get_models_by_tag(self, tag: str) -> List[str]:
        models = self.client.search_registered_models(filter_string=f"tag.value = {tag}")
        return [model.name for model in models]

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
