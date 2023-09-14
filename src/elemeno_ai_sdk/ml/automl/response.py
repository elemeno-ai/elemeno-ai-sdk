from dataclasses import dataclass
from typing import List

from src.elemeno_ai_sdk.ml.automl.request import AutoMLRequest


@dataclass
class RunAutoMLResponse:
    job_id: str


@dataclass
class GetJobResponse:
    request: AutoMLRequest
    status: str
    artifactUri: str


@dataclass
class ListJobsResponse:
    resources: List[AutoMLRequest]
