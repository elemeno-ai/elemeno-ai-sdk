import io
import os
import json
import pickle
from typing import Any, Dict, Optional

import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

from elemeno_ai_sdk.logger import logger
from elemeno_ai_sdk.ml.mlhub_client import MLHubRemote
import matplotlib.pyplot as plt


class AutoMLClient(MLHubRemote):
    def __init__(self, env: Optional[str] = None) -> None:
        super().__init__(env=env)

    async def list_jobs(self) -> Dict[str, Any]:
        response = await self.get(url=f"{self.base_url}/automl")
        return response["resources"]

    async def get_job(self, job_id: str) -> Dict[str, Any]:
        response = await self.get(url=f"{self.base_url}/automl/{job_id}")
        return response

    async def run_job(
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
        to_balance: bool = False
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
            "toBalance": to_balance,
        }
        if features_selected != "":
            body["featuresSelected"] = features_selected

        try:
            response = await self.post(url=f"{self.base_url}/automl", body=body)
            return json.loads(response)
        except Exception:
            logger.exception("Failed to run job.")

    async def get_metadata(self, job_id: str) -> Dict[str, Any]:
        return await self.get(url=f"{self.base_url}/automl/{job_id}/metrics")

    async def get_model(self, job_id: str, save_path: str = ".") -> None:
        response = await self.get(url=f"{self.base_url}/automl/{job_id}/modelfile", is_binary=True)
        if not os.path.exists(save_path):
            os.mkdir(save_path)
        with open(f"{save_path}/model.pkl", "wb") as model_file:
            model_file.write(response)

    async def get_dataset(self, job_id: str, save_path: str = ".") -> None:
        for filetype in ["train", "test"]:
            data_bin = await self.get(url=f"{self.base_url}/automl/{job_id}/dataset/{filetype}", is_binary=True)
            data = pd.read_csv(io.BytesIO(data_bin))
            if not os.path.exists(save_path):
                os.mkdir(save_path)
            data.to_csv(f"{save_path}/{filetype}_data.csv", index=False, )

    async def thresholds(self, job_id: str) -> None:
        metadata = await self.get_metadata(job_id=job_id)
        label_column = metadata["target"] if metadata["target"] is not None else 'class'  # modify endpoint at SaaSBack for return correct target key

        await self.get_model(job_id=job_id, save_path=job_id)
        model = pickle.load(open(f"{job_id}/model.pkl", "rb"))

        await self.get_dataset(job_id=job_id, save_path=job_id)
        train_dataset = pd.read_csv(f"{job_id}/train_data.csv")
        y_true = train_dataset.pop(label_column)

        y_pred_prob = model.predict_proba(train_dataset)

        pred_scores = {}
        scores = [accuracy_score, balanced_accuracy_score, roc_auc_score]
        for score in scores:
            pred_scores[f"{score.__name__}"] = []
            for i in range(1, 101, 1):
                y_pred = (y_pred_prob[:, 1] >= i/100).astype(int)
                pred_scores[f"{score.__name__}"].append(round(score(y_true, y_pred), 4))

        pred_scores["class_eval"] = []
        for i in range(1, 101, 1):
            y_pred = (y_pred_prob[:, 1] >= i/100).astype(int)
            f1 = round(f1_score(y_true, y_pred), 4)
            recall = round(recall_score(y_true, y_pred), 4)
            precision = round(precision_score(y_true, y_pred), 4)
            pred_scores["class_eval"].append([f1, recall, precision])

        return pred_scores

    async def plot_thresholds(self, job_id: str) -> None:
        thresholds = await self.thresholds(job_id=job_id)
        fig, axs = plt.subplots(len(thresholds), 1, sharex=True, figsize=(10, 15))
        for i, key in enumerate(thresholds.keys()):
            axs[i].plot(range(1, 101, 1), thresholds[key])
            axs[i].set_ylabel(f'{key} ({max(thresholds[key])})')
            axs[i].set_xlabel(f'Max {key}: {thresholds[key].index(max(thresholds[key]))/100}')
            if key == 'class_eval':
                axs[i].legend(["f1", "recall", "precision"])
        plt.show()
