
from typing import Any
from elemeno_ai_sdk.models.conversion.converter_abc import ConverterABC
import pickle
import skl2onnx
from skl2onnx.common.data_types import FloatTensorType

ConverterABC.register


class SklearnConverter:

    def transform(self, path: str) -> Any:
        onnx_path = path + ".onnx"
        with open(path, 'rb') as file:
            model = pickle.load(file)
            initial_type = [('float_input', FloatTensorType([1, 4]))]
            onx = skl2onnx.convert_sklearn(model, initial_types=initial_type)
            with open(onnx_path, "wb") as f:
                f.write(onx.SerializeToString())