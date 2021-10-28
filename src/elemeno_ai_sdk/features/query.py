import typing
import gcsfs
import pandas as pd
from urllib.parse import urlparse
from pyarrow.parquet import ParquetDataset
from elemeno_ai_sdk.features.feature_store import FeatureStore
from elemeno_ai_sdk.features.definition import FeatureTableDefinition

class Query:
    
    def __init__(self, feature_store: FeatureStore, 
                 definition: FeatureTableDefinition):
        self._feature_store = feature_store
        self._definition = definition
        
    def get_historical_features(self, entities_where: pd.DataFrame):
        ft = self._definition
        entities = ft.entities
        cols = [p.name for p in entities]
        source_entity = pd.DataFrame(columns=cols)
        input_cols = entities_where.columns
        for c in cols:
            if c not in input_cols:
                raise ValueError(f"Invalid input. The column {c} is missing from the where object")
            source_entity[c] = entities_where[c]
        if not ft.evt_col in input_cols:
            raise ValueError("Missing the event timestamp column in input")
        source_entity[ft.evt_col] = entities_where[ft.evt_col]
        features = [f"{ft.name}:{fd.name}" for fd in ft.features]
        return self._feature_store.get_historical_features(feature_refs=features, entity_source=source_entity)
    
    def to_results_df(self, job):
        return self._read_parquet(job.get_output_file_uri())
    
    def _read_parquet(self, uri):
        parsed_uri = urlparse(uri)
        if parsed_uri.scheme == "file":
            return pd.read_parquet(parsed_uri.path)
        elif parsed_uri.scheme == "gs":
            fs = gcsfs.GCSFileSystem()
            files = ["gs://" + path for path in fs.glob(uri + '/part-*')]
            ds = ParquetDataset(files, filesystem=fs)
            return ds.read().to_pandas()
        elif parsed_uri.scheme == 's3':
            import s3fs
            fs = s3fs.S3FileSystem()
            files = ["s3://" + path for path in fs.glob(uri + '/part-*')]
            ds = ParquetDataset(files, filesystem=fs)
            return ds.read().to_pandas()
        elif parsed_uri.scheme == 'wasbs':
            import adlfs
            fs = adlfs.AzureBlobFileSystem(
                account_name=os.getenv('FEAST_AZURE_BLOB_ACCOUNT_NAME'), account_key=os.getenv('FEAST_AZURE_BLOB_ACCOUNT_ACCESS_KEY')
            )
            uripath = parsed_uri.username + parsed_uri.path
            files = fs.glob(uripath + '/part-*')
            ds = ParquetDataset(files, filesystem=fs)
            return ds.read().to_pandas()
        else:
            raise ValueError(f"Unsupported URL scheme {uri}")