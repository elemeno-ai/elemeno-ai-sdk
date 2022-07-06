***************
Getting Started
***************

Overview
########

Elemeno AI SDK is the one stop shop for all the elements needed to build your own AI engine.

It includes helpers to use the Elemeno AI operating system, and supports both Elemeno Serverless AI and local installations.

Current features available in the SDK:

- Feature Store Management
- Data Ingestion
  
  - Big Query Datasource
  - Redshift Datasource
  - Elasticsearch Datasource
  - Pandas DF Datasource

- Training Data Reading
- Inference Data Reading
- ML Frameworks Conversion to ONNX
  
  - Scikit-learn
  - Tensorflow
  - Pytorch
  - Tensorflow-Lite
- Authentication Utils


First Steps
************

The first step is to install the SDK module via pip.

.. code:: bash

  pip install elemeno-ai-sdk


You then run the command
:code `mlops init` 
and follow the steps in the terminal to configure your MLOps environment.

That's all.

(optional) If you intend to leave the configuration files in a location different from the default, set the environment variable below.

.. code:: bash

  export ELEMENO_CFG_FILE=<path to config directory>


Configuration file schema
*************************

A configuration file named elemeno.yaml is expected to be present in the root of the project (or where the variable ELEMENO_CFG_FILE points to).

The file has the following structure:

.. list-table:: Config File Structure
    :widths: 30 30 60 80
    :header-rows: 1

    * - Field
      - Type
      - Example
      - Description
    * - app
      - object 
      - 
      - The general application configuration
    * - app.mode
      - string
      - development
      - The execution mode, use development for local development and production when doing an oficial run.
    * - cos
      - object
      - 
      - The S3-like Cloud Object Storage configuration. This is where your artifacts will be persisted. The bucket with name elemeno-cos should exist.
    * - cos.host
      - string
      - http://minio.example.com:9000
      - The host of the cloud object storage server.
    * - cos.key_id
      - string
      - AKIAIOSFODNN7EXAMPLE
      - The access key id for the cloud object storage server.
    * - cos.secret
      - string
      - wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
      - The secret access key for the cloud object storage server.
    * - cos.use_ssl
      - boolean
      - true
      - Whether to use SSL or not.
    * - registry
      - object
      - 
      - The model registry configuration. Currently Elemeno supports MLFlow as registry.
    * - registry.tracking_url
      - string
      - http://mlflow.tracking.url:80
      - The MLFlow tracking server url.
    * - feature_store
      - object
      - 
      - The feature store configuration. Currently Elemeno supports Feast as feature store.
    * - feature_store.feast_config_path
      - string
      - .
      - The path to the Feast configuration file.
    * - feature_store.registry
      - string
      - s3://elemeno-cos/example_registry
      - The path in the cloud object storage to keep the metadata of the feature store.
    * - feature_store.sink
      - object
      - 
      - The sink configuration. Currently Elemeno supports Redshift and BigQuery as sink.
    * - feature_store.sink.type
      - string
      - Redshift
      - The type of the sink.
    * - feature_store.sink.params
      - object
      - 
      - The parameters of the sink.
    * - feature_store.sink.params.user
      - string
      - elemeno
      - The user name for the Redshift database.
    * - feature_store.sink.params.password
      - string
      - ${oc.env:REDSHIFT_PASSWORD,elemeno}
      - The password for the Redshift database.
    * - feature_store.sink.params.host
      - string
      - cluster.host.on.aws
      - The host of the Redshift database cluster.
    * - feature_store.sink.params.port
      - integer
      - 5439
      - The port of the Redshift database cluster.
    * - feature_store.sink.params.database
      - string
      - elemeno
      - The name of the Redshift database schema.
    * - feature_store.source
      - object
      - 
      - The data source configuration. Currently Elemeno supports Elasticsearch, Pandas, Redshift and BigQuery as source.
    * - feature_store.source.type
      - string
      - BigQuery
      - The type of the data source. Valid values are BigQuery, Elastic and Redshift
    * - feature_store.source.params (When using Elastic as source)
      - object
      - 
      - The parameters of the data source.
    * - feature_store.source.params.host
      - string
      - localhost:9200
      - The host of the Elasticsearch server.
    * - feature_store.source.params.user
      - string
      - elemeno
      - The user name for the Elasticsearch server.
    * - feature_store.source.params.password
      - string
      - ${oc.env:ELASTIC_PASSWORD,elemeno}
      - The password for the Elasticsearch server.
    * - feature_store.source.params (When using Redshift as source)
      - object
      - 
      - The parameters of the Redshift data source.
    * - feature_store.source.params.cluster_name
      - string
      - elemeno
      - The name of the Redshift cluster on AWS. When this parameter is specified the SDK uses IAM-based authentication, therefore it's not needed to specify host, port, user and password
    * - feature_store.source.params.user
      - string
      - elemeno
      - The user name for the Redshift database.
    * - feature_store.source.params.password
      - string
      - ${oc.env:REDSHIFT_PASSWORD,elemeno}
      - The password for the Redshift database.
    * - feature_store.source.params.host
      - string
      - cluster.host.on.aws
      - The host of the Redshift database cluster.
    * - feature_store.source.params.port
      - integer
      - 5439
      - The port of the Redshift database cluster.
    * - feature_store.source.params.database
      - string
      - elemeno
      - The name of the Redshift database schema.
    * - feature_store.source.params (When using BigQuery as source)
      - object
      - 
      - The parameters of the data BigQuery source.
    * - feature_store.source.params.project_id
      - string
      - elemeno
      - The project id of the BigQuery project.