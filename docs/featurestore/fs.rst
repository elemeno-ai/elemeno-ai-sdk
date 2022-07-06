*************
Feature Store
*************

Getting Started
***************

The feature store is a powerful tool for ML practitioners. It abstracts away many of the complexities
involved in the data engineering architecture to support both training and inference time.

Through this class, you can interact with Elemeno feature store from your notebooks and applications.

Here is a simple example of how to create a feature table in the feature store:

.. code-block:: python
  
  feature_store = FeatureStore(sink_type=IngestionSinkType.REDSHIFT, connection_string=conn_str)
  feature_table = FeatureTable("my_test_table", feature_store)

  feature_store.ingest_schema(feature_table, "path_to_schema.json")

In the above snippet we did a few things that are necessary to start using the feature store.

1. We instantiated the FeatureStore object, specifying which type of sink we want to use. Sinks is the terminology used by the feature store to refer to the different types of data stores that it supports.
2. We instantiated a FeatureTable object, which is a wrapper around the feature store table.
3. We ingested the schema for the feature table using the ingest_schema method.


Reference
*********

.. autoclass:: elemeno_ai_sdk.ml.features.feature_store.FeatureStore
  :special-members: __init__
  :members:

.. toctree::
   :maxdepth: 3
   :caption: Classes:

   ingest/sink/index
   ingest/source/index
