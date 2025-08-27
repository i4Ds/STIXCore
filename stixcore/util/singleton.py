__all__ = ["Singleton"]


class Singleton(type):
    def __init__(cls, *args, **kwargs):
        cls._instance = None

    @property
    def instance(cls):
        if cls._instance is None:
            raise ValueError("Singleton not initialized")
        return cls._instance

    @instance.setter
    def instance(cls, value):
        if not isinstance(value, cls):
            raise ValueError(f"Singleton must be of type: {cls}")
        cls._instance = value
