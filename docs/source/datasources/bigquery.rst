*********
Big Query
*********

Authenticating
##############

This SDK includes google-sdk for dealing programatically with BigQuery on Google Cloud Platform. A common use of google-sdk is within notebooks, by users that may make their notebooks into pipelines at some point. We assume that people would prefer to use their personal accounts when working in the notebook, but use a service account when running pipelines in an automated fashion. The SDK turns the process of selecting which type of authentication to use transparent.

In order to generate the credentials to then use alongside google SDK you could:

.. code-block:: python
   
   from elemeno_ai_sdk.datasources.gcp.google_auth import Authenticator
   auth = Authenticator()
   credentials = auth.get_credentials()

The *credentials* variable can then be passed around on google-sdk methods.

In order to configure different values to the authenticator, edit the following section of the file elemeno.yaml

.. code-block:: yaml

   ...
   gcp:
   sa:
     file: /tmp/gcp-credentials.json
   appflow:
     client_secret:
       file: /tmp/client_secrets.json
     scopes:
       - 'https://www.googleapis.com/auth/bigquery'
   ...

If you need help generating the client_secrets.json file, see `Google documentation`_. 

.. _Google documentation: https://cloud.google.com/bigquery/docs/authentication/end-user-installed#manually-creating-credentials

