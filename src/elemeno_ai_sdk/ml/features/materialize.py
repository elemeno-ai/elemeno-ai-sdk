from datetime import datetime
from feast.feature_store import FeatureStore
from feast import utils
from elemeno_ai_sdk.config import Configs

DATETIME_ISO = "%Y-%m-%dT%H:%M:%s"

elm_config = Configs.instance()

def materialize_incremental(feature_view: str,
                            end_ts=datetime.now().replace(microsecond=0).isoformat()):
  
  store = FeatureStore(elm_config.feature_store.feast_config_path)
  store.materialize_incremental(
    feature_views=[feature_view],
    end_date=utils.make_tzaware(datetime.fromisoformat(end_ts)),
  )
