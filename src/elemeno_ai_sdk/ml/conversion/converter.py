
import logging
from typing import Optional, Tuple
from elemeno_ai_sdk.ml.conversion.tflite import TFLiteConverter
from elemeno_ai_sdk.ml.conversion.sklearn import SklearnConverter
from elemeno_ai_sdk.ml.conversion.tensorflow import TensorflowConverter
from elemeno_ai_sdk.ml.conversion.torch import TorchConverter

class ModelConverter:

  def __init__(self, model_path: str, input_dims: Optional[Tuple[int]]):
    self.model_path = model_path
    self.input_dims = input_dims

  def apply_conversion(self) -> None:
    """
    Apply ONNX conversion for the model.
    
    :return: None
    :rtype: None
    """
    file_extension = self.model_path.split('.')[-1]
    if file_extension == 'onnx':
      logging.info('Nohting to do, model is already onnx')
      return
    converters = {
      'h5': TensorflowConverter(),
      'pkl': SklearnConverter(),
      'tflite': TFLiteConverter(),
      'pt': TorchConverter()
    }
    if file_extension not in converters:
      raise ValueError(f'unsupported file extension: {file_extension}')
    
    if file_extension == 'pt':
      converters[file_extension].transform_with_dims(self.model_path, self.input_dims)
      return
    converters[file_extension].transform(self.model_path)
    return