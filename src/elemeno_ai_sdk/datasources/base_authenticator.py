import abc

class BaseAuthenticator(metaclass=abc.ABCMeta):
    
    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'authenticate') and
                callable(subclass.authenticate))

