*************
Feature Store
*************

Overview
########

The usage of a feature store solution has many advantages for machine learning projects that need to deliver results in production. Through a standardized architecture, teams are able to move faster throughout the whole development workflow at the same time that achieve gold standards several requirements in production.

Elemeno.ai SDK has many built in methods that make the usage of a feature store a smooth process.

Getting started
***************

The first is to create a client connection to the feature store server. In this example we use the feature store provided by Elemeno.ai SaaS. You may be also to connect to your own on premises feature store by changing a few parameters.

First, import the necessary classes

.. code-block:: python

  import feast
  from elemeno_ai_sdk.features.feature_store import FeatureStore
  from elemeno_ai_sdk.features.definition import FeatureTableDefinition, BigQueryDataSource

Then create the feature store client and connect

.. code-block:: python

  fs = FeatureStore()
  fs.connect()

Now that you have a connected feature store object you can create your feature tables and feature entities. A feature entity is the definition of a feature key, i.e. the data used to retrieve feature samples. The feature table is a collection of features entities and features. To create a FeatureTableDefinition object follow the example

.. code-block:: python

   feature_table = FeatureTableDefinition(
    name = "items_v2", event_column = "event_timestamp",
    batch_source = BigQueryDataSource(
        project="elemeno-internal", 
        dataset="feast_ingest_test",
        credentials=credentials),
    feature_store = fs)
   
A timestamp column, representing the time when the event associated to the feature creation happened, is mandatory and it provides the ability to the user to ensure reproducibility and also valuable debugging techniques, like time-traveling. Once you have the FeatureTableDefinition, it's time to define the schema of your FeatureTable

.. code-block:: python

   feature_table.register_entity(feast.Entity(name="id", description="Product ID", value_type=feast.ValueType.STRING))

   feature_table.register_features(feast.Feature("len_name", feast.ValueType.INT64),
                               feast.Feature("len_description", feast.ValueType.INT64),
                               feast.Feature("sellertype_code", feast.ValueType.INT64),
                               feast.Feature("num_sales", feast.ValueType.INT64))
   
   feature_table.apply()

Here we defined a schema that contains one feature entity and four features. The apply method will commit the changes to the server. Once apply is called the feature table is created in the server. You're able to access the client of the feature store server by using `fs.raw_client`. This variable holds a reference to a feast client. Find more useful methods on feast-sdk `documentation`_.

.. _documentation: https://api.docs.feast.dev/python/#module-feast.client
