import asyncio
import pickle
from typing import Dict, Optional

from elemeno_ai_sdk.ml.mlhub_client import MLHubRemote


class AutoMLClient(MLHubRemote):
    def __init__(self, env: Optional[str] = None) -> None:
        super().__init__(env=env)

    def list_jobs(self):
        return self.get(url=f"{self.base_url}/automl")

    def get_job(self, job_id: str):
        return self.get(url=f"{self.base_url}/automl/{job_id}")

    def run_job(
        self,
        feature_table_name: str,
        features_selected: str,
        id_column: str,
        target_name: str,
        start_date: str,
        end_date: str,
        task: str,
        scoring: str,
        num_features: int,
        generations: int,
    ) -> Dict[str, str]:
        body = {
            "featureTableName": feature_table_name,
            "idColumn": id_column,
            "targetName": target_name,
            "startDate": start_date,
            "endDate": end_date,
            "task": task,
            "scoring": scoring,
            "numFeatures": num_features,
            "generations": generations,
        }
        if features_selected != "":
            body["featuresSelected"] = features_selected

        return self.post(url=f"{self.base_url}/automl", body=body)

    def get_metadata(self, job_id: str):
        return self.get(url=f"{self.base_url}/automl/{job_id}/metrics")

    async def get_model(self, job_id: str):
        response = await self.get(url=f"{self.base_url}/automl/{job_id}/modelfile", is_binary=True)
        with open("./model.pkl", "wb") as model_file:
            model_file.write(response)
