import abc
import pickle
from typing import Any

class ConverterABC(abc.ABC):

    @abc.abstractmethod
    def transform(self, file: str) -> Any:
        """
        loads a binary model from disk and returns the bytes

        arguments:
        file: the file path to be loaded

        returns:
        the bytes of the loaded file
        """
        pass

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'transform') and
                callable(subclass.transform))
