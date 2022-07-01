from typing import Any, Tuple
import torch
from elemeno_ai_sdk.ml.conversion.converter_abc import ConverterABC

ConverterABC.register


class TorchConverter:
    """
    Converts a complete torch model to onnx.

    :param path: path to the torch model file
    :param input_dims: input dimensions of the model
    :return: onnx model
    """

    def transform_with_dims(self, path: str, input_dims: Tuple[int] = None) -> Any:
        """
        This function is used to convert a Pytorch model to ONNX format.

        :param path: Path to the Pytorch model
        :type path: str
        :param input_dims: Input dimensions of the model
        :type input_dims: Tuple[int]
        :return: None
        :rtype: Any
        """

        onnx_path = path + '.onnx'
        model = torch.load(path)
        model.eval()
        dummy_input = torch.randn(*input_dims)
        torch.onnx.export(model, dummy_input, onnx_path, verbose=True)