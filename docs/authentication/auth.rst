*******************************************************************************
Authentication Utils
*******************************************************************************

Overview
########

Often times, specially when dealing with the first steps of data engineering, you may need to connect to different 
services in the cloud. We build this module to help you streamline the process of authenticating with some of these services.

Google Cloud
############

There are a few ways to authenticate with Google Cloud SDK. The most common, is to use a service account file and specify 
its location in the environment variable GOOGLE_APPLICATION_CREDENTIALS. However, we understand this type of authentication
requires a lot of overhead to be handled in a secure way, specially if you're not a one-person project. 

For development time, you can use API-based authentication tokens through the google appflow package.

By using the Authenticator class, you can easily just call authentication, and depending on the existence of an
environment variable, PRODUCTION_MODE, it will use the service account file or the API-based authentication tokens.

When PRODUCTION_MODE is True, it will use the service account file. When PRODUCTION_MODE is False, it will use the API-based

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

