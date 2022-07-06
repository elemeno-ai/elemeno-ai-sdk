********************
Feature Store Source
********************

The concept of a Source is how we read data from a lake of data that not necessary contain ML friendly features.

Reference
*********

.. autoclass:: elemeno_ai_sdk.ml.features.ingest.source.ingestion_source_builder.IngestionSourceBuilder
  :special-members: __init__
  :members:

.. autoclass:: elemeno_ai_sdk.ml.features.ingest.source.redshift.RedshiftIngestionSource
  :special-members: __init__
  :members:

.. autoclass:: elemeno_ai_sdk.ml.features.ingest.source.bigquery.BigQueryIngestionSource
  :special-members: __init__
  :members:

.. autoclass:: elemeno_ai_sdk.ml.features.ingest.source.elastic.ElasticIngestionSource
  :special-members: __init__
  :members: