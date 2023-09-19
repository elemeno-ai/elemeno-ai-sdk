from fastapi import FastAPI, Body, Depends
from fastapi.security import APIKeyHeader
import pandas as pd
from pydantic import BaseModel
from typing import Dict, List, Optional

from src.elemeno_ai_sdk.ml.features.feature_store import FeatureStore
from src.elemeno_ai_sdk.ml.features.feature_table import FeatureTable
from src.elemeno_ai_sdk.ml.features.ingest.source.base_source import ReadResponse

app = FastAPI()

from src.elemeno_ai_sdk.ml.features.feature_table import FeatureTable

class FeatureTableRequest(BaseModel):
    name: str
    entities: List[str]
    features: List[str]

class IngestRequest(BaseModel):
    feature_table: FeatureTableRequest
    to_ingest: Dict
    renames: Optional[Dict[str, str]]
    all_columns: Optional[List[str]]

@app.post("/ingest")
def ingest_data(request: IngestRequest):
    feature_table_request = request.feature_table
    feature_table = FeatureTable(
        name=feature_table_request.name,
        feature_store=FeatureStore(),
        entities=[feast.Entity(name=e) for e in feature_table_request.entities],
        features=[feast.Feature(name=f) for f in feature_table_request.features],
    )
    to_ingest = ReadResponse(pd.DataFrame(request.to_ingest), None)
    renames = request.renames
    all_columns = request.all_columns

    feature_store = FeatureStore()
    feature_store.ingest_response(feature_table, to_ingest, renames, all_columns)

    return {"message": "Data ingested successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
