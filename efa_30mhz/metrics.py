import statsd
from loguru import logger


class Metric:
    __instance = None
    __kwargs = None

    @staticmethod
    def initialize_client(**kwargs):
        Metric.__kwargs = kwargs

    @staticmethod
    def client() -> statsd.StatsClient:
        if Metric.__instance is None:
            assert Metric.__kwargs is not None
            Metric.__instance = statsd.StatsClient(**Metric.__kwargs)
        return Metric.__instance
