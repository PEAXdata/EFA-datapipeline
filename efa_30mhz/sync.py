from abc import ABC, abstractmethod
from typing import Dict, Type, List, Callable

from loguru import logger


class Source(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @staticmethod
    @abstractmethod
    def to_thirty_mhz(**kwargs):
        pass

    @abstractmethod
    def read_all(self) -> List:
        return []


class Target(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def write(self, rows):
        pass


class Sync:
    """
    A Sync object represents a continuous synchronization between a source and a target.
    """

    def __init__(self, source: Source, target: Target) -> None:
        self.source = source
        self.target = target

    def start(self):
        rows = self.source.read_all()
        logger.info('Converting data to 30MHz format')
        thirty_mhz_rows = self.source.to_thirty_mhz(rows)
        logger.info(f'Writing data')
        logger.debug(thirty_mhz_rows)
        self.target.write(thirty_mhz_rows)
