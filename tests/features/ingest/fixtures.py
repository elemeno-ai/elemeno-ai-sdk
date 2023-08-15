import os

import pandas as pd
import pytest

from elemeno_ai_sdk.ml.features.feature_store import FeatureStore
from elemeno_ai_sdk.ml.features.feature_table import FeatureTable


@pytest.fixture
def feature_store():
    os.environ["ELEMENO_CFG_FILE"] = "./elemeno.yaml.TEMPLATE"
    f = FeatureStore(sink_type="RedshiftUnitTests", source_type=None)
    return f


@pytest.fixture
def feature_table(feature_store):
    ft = FeatureTable(feature_store=feature_store, name="test_table")
    feature_store.ingest_schema(ft, "tests/schema/test_schema.json")
    return ft


@pytest.fixture
def with_test_row(feature_table, feature_store):
    feature_store.ingest(
        feature_table,
        pd.DataFrame(
            [
                {
                    "listingId": 1,
                    "aggregationIds": 123,
                    "created_timestamp": "2020-01-01T00:00:01",
                    "event_timestamp": "2020-01-01T00:00:01",
                },
                {
                    "listingId": 2,
                    "aggregationIds": 123,
                    "created_timestamp": "2019-12-01T00:00:01",
                    "event_timestamp": "2019-11-28T00:00:01",
                },
            ]
        ),
    )
