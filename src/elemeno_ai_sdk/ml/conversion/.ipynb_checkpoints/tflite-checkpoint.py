from typing import Any
import tensorflow as tf
import tf2onnx
from elemeno_ai_sdk.models.conversion.converter import ConverterABC

ConverterABC.register

class TFLiteConverter:
    
    def transform(self, path: str, model_name: str = "elemeno") -> Any:
        tf.compat.v1.disable_eager_execution()
        onnx_path = path + ".onnx"
        with tf.compat.v1.Session() as sess:
            interpreter = tf.lite.Interpreter(model_path=path)
            x = tf.compat.v1.placeholder(tf.float32, [2, 3], name="input")
            x_ = tf.add(x, x)
            _ = tf.identity(x_, name="output")
            onnx_graph = tf2onnx.tfonnx.process_tf_graph(sess.graph, 
                                                         input_names=["input:0"], 
                                                         output_names=["output:0"])
            model_proto = onnx_graph.make_model(model_name)
            with open(onnx_path, "wb") as f:
                f.write(model_proto.SerializeToString())

