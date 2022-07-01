from typing import Any
import tf2onnx
from tensorflow.python.keras.models import load_model
import onnx
from elemeno_ai_sdk.ml.conversion.converter_abc import ConverterABC

ConverterABC.register


class TensorflowConverter:
    """
    Transform a tensorflow model to a onnx model

    :param path: the path to the tensorflow model
    :return: the path to the onnx model
    """

    def transform(self, path: str) -> Any:
        """
        This function takes a path to a model file (in .h5 format) and converts it to ONNX format

        Parameters
        ----------
        path : str
            Path to the model file in .h5 format

        Returns
        -------
        Any
            Returns the path to the converted model file in ONNX format
        """
        onnx_path = path + '.onnx'

        model = load_model(path)
        model_proto, external_tensor_storage = tf2onnx.convert.from_keras(model,
                        input_signature=None, opset=None, custom_ops=None,
                        custom_op_handlers=None, custom_rewriter=None,
                        inputs_as_nchw=None, extra_opset=None, shape_override=None,
                        target=None, large_model=False, output_path=None)
        onnx.save_model(model_proto, onnx_path)