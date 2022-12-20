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


Ingesting Features
******************

Once you have created a feature table, you can start ingesting features into it. The feature store supports two types of ingestion: batch and streaming (WIP).

Let's imagine you have your own feature engineering pipeline that produces a set of features for a given entity. You can use the feature store to ingest these features into the feature store.

.. code-block:: python

  # this is a pandas dataframe
  df = my_own_feature_engineering_pipeline()

  fs.ingest(feature_table, df)

That's all that's needed. There are some extra options you can pass to the ingest method, but this is the simplest way to ingest features into the feature store.


Reading Features
****************

Once you have ingested features into the feature store, you can start reading them. The feature store supports two types of reads: batch and online.

The batch read is what you will usually need during training. It allows you to read a set of features for a given entity over a given time range.

.. code-block:: python

  # the result is a pandas dataframe
  training_df = fs.get_training_features(feature_table, date_from="2023-01-01", date_to="2023-01-31", limit=5000)
  

For the online read, you can use the get_online_features method. This method will return an OnlineResponse object of features for a given entity. This type of object has a to_dict method that can be used to convert the features into a dictionary.

.. code-block:: python

  entities = [{"user_id": "1234"},{"user_id": "5678"}]

  # the result is an OnlineResponse object, with all the features associated with the given entities
  features = fs.get_online_features(entities)


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
