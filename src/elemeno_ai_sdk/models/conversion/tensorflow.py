from typing import Any
import tf2onnx
from tensorflow.python.keras.models import load_model
import onnx
from elemeno_ai_sdk.models.conversion.converter import ConverterABC

ConverterABC.register


class TensorflowConverter:

    def transform(self, path: str) -> Any:
        """
        Convert a tensorflow model to onnx model
        :param file_name: (a path to the .h5 file)
        :return:
        """
        
        onnx_path = path + '.onnx'

        model = load_model(path)
        model_proto, external_tensor_storage = tf2onnx.convert.from_keras(model,
                        input_signature=None, opset=None, custom_ops=None,
                        custom_op_handlers=None, custom_rewriter=None,
                        inputs_as_nchw=None, extra_opset=None, shape_override=None,
                        target=None, large_model=False, output_path=None)
        onnx.save_model(model_proto, onnx_path)