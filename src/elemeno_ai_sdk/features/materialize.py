from datetime import datetime
from feast.feature_store import FeatureStore
from feast import utils

DATETIME_ISO = "%Y-%m-%dT%H:%M:%s"

def materialize_incremental(feature_view: str,
                            repo_path: str,
                            end_ts=datetime.now().replace(microsecond=0).isoformat()):
  store = FeatureStore(repo_path)
  store.materialize_incremental(
    feature_views=[feature_view],
    end_date=utils.make_tzaware(datetime.fromisoformat(end_ts)),
  )
