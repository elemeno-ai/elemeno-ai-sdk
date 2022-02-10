from typing import Any, Tuple
import torch
from elemeno_ai_sdk.models.conversion.converter_abc import ConverterABC

ConverterABC.register


class TorchConverter:

    def transform_with_dims(self, path: str, input_dims: Tuple[int] = None) -> Any:
        """
        Transforms a fully saved model from torch (not a state_dict)
        to a onnx file.

        Arguments:
        path: str - The path to the .pt file
        input_dims: Tuple[int] - A tuple containing the input dimensions for the model

        Returns the onnx model
        """

        onnx_path = path + '.onnx'
        model = torch.load(path)
        model.eval()
        dummy_input = torch.randn(*input_dims)
        torch.onnx.export(model, dummy_input, onnx_path, verbose=True)