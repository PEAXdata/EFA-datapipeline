from abc import ABC, abstractmethod
from typing import Dict, Type, List


class Source(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def __enter__(self):
        return self

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    def read_all(self) -> List:
        return []


class Target(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def __enter__(self):
        return self

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    def write(self, row):
        pass


class Sync:
    """
    A Sync object represents a continuous synchronization between a source and a target.
    """

    def __init__(self, source_class: Type[Source], source_kwargs: Dict, target_class: Type[Target],
                 target_kwargs: Dict) -> None:
        self.source_class = source_class
        self.source_kwargs = source_kwargs
        self.target_class = target_class
        self.target_kwargs = target_kwargs

    def start(self):
        with self.source_class(**self.source_kwargs) as source:
            rows = source.read_all()
        with self.target_class(**self.target_kwargs) as target:
            for row in rows:
                target.write(row)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
