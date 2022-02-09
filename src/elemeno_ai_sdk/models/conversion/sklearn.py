
from typing import Any
from elemeno_ai_sdk.models.conversion.converter import ConverterABC
import pickle
#from onnx import save_model
import skl2onnx

ConverterABC.register
class SklearnConverter:

    def transform(self, file: str) -> Any:
        # Load the model
        model = pickle.load(open(file, 'rb'))
        # Convert the model
        onnx_model = skl2onnx.convert_sklearn(model, 'model')
        # Save as protobuf
        #save_model(onnx_model, 'model.onnx')
        return onnx_model

if __name__ == '__main__':
    s = SklearnConverter()
    s.transform('sklearn_classifier.pickle')