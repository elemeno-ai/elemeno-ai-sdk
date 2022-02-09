from typing import Any
import pickle
import onnxmltools
from elemeno_ai_sdk.models.conversion.converter import ConverterABC

ConverterABC.register
class TensorflowConverter:

    def transform(self, file: str) -> Any:
        """
        Convert a tensorflow model to onnx model
        :param file_name:
        :return:
        """
        # Load the model
        model = self._load_tf_from_file(file)
        # Convert the model
        onnx_model = onnxmltools.convert_tensorflow(model)
        # Save as protobuf
        onnxmltools.utils.save_model(onnx_model, file + '.onnx')

    def _load_tf_from_file(self, file) -> Any:
        with open(file, 'rb') as f:
            return pickle.load(f)