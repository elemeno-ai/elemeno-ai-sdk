import abc


class IFineTune(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_body(self):
        return NotImplemented

    @abc.abstractmethod
    def get_file(self):
        return NotImplemented

    @abc.abstractmethod
    def get_endpoint(self):
        return NotImplemented
