****************
Model Conversion
****************

Overview
========

In order to deploy your ML models you usually need to first serialize them to a format that can be consumed by the ML service.
At Elemeno ML Ops, we currently support native deployments of Tensorflow (and TFLite), Pytorch, Scikit-learn and Keras.

However, if you're looking for the maximum performance optimization, we have built a base server in GoLang, that is able
to respond inference requests in very few ms of latency.

For users looking to deploy using Elemeno MLOps optimized server you will need to first convert your binary model to
the open standard `ONNX <https://onnx.ai>`_ . Check below the SDK components that will help you on doing a frictionless conversion.


Reference
=========

.. .. autoclass:: elemeno_ai_sdk.ml.conversion.converter.ModelConverter
..   :members:

.. .. autoclass:: elemeno_ai_sdk.ml.conversion.sklearn.SklearnConverter
..   :members:

.. .. autoclass:: elemeno_ai_sdk.ml.conversion.tensorflow.TensorflowConverter
..   :members:

.. .. autoclass:: elemeno_ai_sdk.ml.conversion.tflite.TFLiteConverter
..   :members:

.. .. autoclass:: elemeno_ai_sdk.ml.conversion.torch.TorchConverter
..   :members: