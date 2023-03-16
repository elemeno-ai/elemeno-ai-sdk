import pytest
import os
import onnxruntime as rt
import numpy


base_path = os.path.dirname(os.path.abspath(__file__))

@pytest.mark.skip(reason="this is breaking in macos with m1 (arm). TODO fix this")
def test_convert_h5():
  from elemeno_ai_sdk.models.conversion.tensorflow import TensorflowConverter
  conv = TensorflowConverter()
  conv.transform(f'{base_path}/mnist_classifier.h5')
  assert(os.path.exists(f'{base_path}/mnist_classifier.h5.onnx'))

@pytest.mark.skip(reason="this is breaking in macos with m1 (arm). TODO fix this")
def test_predict_on_converted():
  sess = rt.InferenceSession(f'{base_path}/mnist_classifier.h5.onnx')
  input_name = sess.get_inputs()[0].name
  print(input_name)
  label = sess.get_outputs()[0].name
  print(label)
  pred_onx = sess.run([label], {input_name: numpy.array(X_test).astype(numpy.float32)})