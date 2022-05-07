import abc
import pickle
from typing import Any, Tuple

class ConverterABC(abc.ABC):

    @abc.abstractmethod
    def transform(self, path: str) -> None:
        """
        loads a binary model from disk and returns the bytes

        arguments:
        path: the file path to be loaded

        """
        pass
    
    @abc.abstractmethod
    def transform_with_dims(self, path: str, input_dims: Tuple[int] = None) \
            -> None:
        """
        loads a binary model from disk and returns the bytes
        to be used when the source framework requires
        the input_dims to be passed
        
        arguments:
        path: str - the file path to be loaded
        input_dims: str - a tuple with the input dimensions for loading the model

        """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (
            (hasattr(subclass, 'transform') and
                callable(subclass.transform)) or
            (hasattr(subclass, 'transform_with_dims') and
                callable(subclass.transform_with_dims))
        )
