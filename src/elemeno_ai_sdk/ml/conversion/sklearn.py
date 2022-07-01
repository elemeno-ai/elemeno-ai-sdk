
from typing import Any
from elemeno_ai_sdk.ml.conversion.converter_abc import ConverterABC
import pickle
import skl2onnx
from skl2onnx.common.data_types import FloatTensorType

ConverterABC.register


class SklearnConverter:
    """
    Convert a pickled sklearn model to onnx.

    Parameters
    ----------
    path: str
        Path to the pickled sklearn model.

    Returns
    -------
    Any
        The onnx model.
    """
    def transform(self, path: str) -> Any:
        """
        This function takes a path to a pickled sklearn model and converts it to
        onnx.

        Parameters
        ----------
        path: str
            The path to the pickled sklearn model.

        Returns
        -------
        Any
            The path to the converted onnx model.
        """
        onnx_path = path + ".onnx"
        with open(path, 'rb') as file:
            model = pickle.load(file)
            initial_type = [('float_input', FloatTensorType([1, 4]))]
            onx = skl2onnx.convert_sklearn(model, initial_types=initial_type)
            with open(onnx_path, "wb") as f:
                f.write(onx.SerializeToString())