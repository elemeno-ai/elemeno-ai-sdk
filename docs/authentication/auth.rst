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
requires some overhead to be handled in a secure way, specially if you're not in an one-person project. 

For development time, you can use API-based authentication tokens through the google appflow package. Hence using service accounts only for production environments.

By using the Authenticator class, you can easily just call authentication, and depending on the existence of a configuration
in the elemeno config yaml. The value of the config app.mode is what switches the behavior of what the authenticator class will use.
When *development*, it will use appflow (user credentials based) authentication. When *production* it will use the service account file or the API-based authentication tokens specified in GOOGLE_APPLICATION_CREDENTIALS.

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

***
AWS
***

For AWS we recommend you use IAM authentication when possible. If you're running your workloads in Elemeno MLOps cloud, there's an option
to generate IAM credentials for AWS integration, and then you can use that *arn* to allow necessary permissions on your account.

If using the opensource version of Elemeno, you can use the IAM roles for service accounts approach. `Learn more <https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html>`_

An easier setup would be to just use the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables. Or even the ~.aws/credentials file.