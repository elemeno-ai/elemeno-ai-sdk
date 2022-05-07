import os
import onnxruntime as rt
import numpy
from elemeno_ai_sdk.models.conversion.tensorflow import TensorflowConverter

base_path = os.path.dirname(os.path.abspath(__file__))

def test_convert_h5():
  conv = TensorflowConverter()
  conv.transform(f'{base_path}/mnist_classifier.h5')
  assert(os.path.exists(f'{base_path}/mnist_classifier.h5.onnx'))

def test_predict_on_converted():
  sess = rt.InferenceSession(f'{base_path}/mnist_classifier.h5.onnx')
  input_name = sess.get_inputs()[0].name
  print(input_name)
  label = sess.get_outputs()[0].name
  print(label)
  pred_onx = sess.run([label], {input_name: numpy.array(X_test).astype(numpy.float32)})