
from typing import Dict, Optional

from elemeno_ai_sdk.ml.mlhub_client import MLHubRemote


class AutoFeaturesClient(MLHubRemote):
    def __init__(self, env: Optional[str] = None) -> None:
        super().__init__(env=env)

    def run_job(
        self,
        df: 'pd.DataFrame',
        ml_task: str = None,
        target_column: str = None,
        ignore_columns: str = None,
    ) -> Dict:
        body = {
            "data": df.to_json(),
            "ml_task": ml_task if ml_task is not None else "",
            "target_column": target_column if target_column is not None else "",
            "ignore_columns": ignore_columns if ignore_columns is not None else "",
        }

        return self.post(url=f"{self.base_url}/autofeatures", body=body)
