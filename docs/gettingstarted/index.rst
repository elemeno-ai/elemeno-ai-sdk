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

.. code-block:: bash
  pip install elemeno-ai-sdk

You then run the command
:code `mlops init` 
and follow the steps in the terminal to configure your MLOps environment.

That's all.

(optional) If you intend to leave the configuration files in a location different from the default, set the environment variable below.

.. code-block:: bash
  export ELEMENO_CFG_FILE=<path to config directory>

