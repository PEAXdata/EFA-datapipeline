import logging
from logging import ERROR, DEBUG

import click
import sentry_sdk
from loguru import logger
import yaml
from sentry_sdk.integrations.logging import BreadcrumbHandler, EventHandler

from efa_30mhz import constants
from efa_30mhz.eurofins import EurofinsSource
from efa_30mhz.json import JSONSource
from efa_30mhz.metrics import Metric
from efa_30mhz.mssql import MSSQLSource
from efa_30mhz.sync import Sync, Source, Target
from efa_30mhz.thirty_mhz import ThirtyMHzTarget

CONFIG_FILE = "config.yaml"


@click.group()
@click.option("--debug/--no-debug", default=False)
@logger.catch
def cli(debug):
    logger.info("Debug mode is %s" % ("on" if debug else "off"))


def parse_config(config_file = None):
    if config_file is None:
        config_file = CONFIG_FILE
    with open(config_file, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return config


def create_mssql_source(database_config):
    def _create_mssql_source(**kwargs):
        return MSSQLSource(**database_config, **kwargs)

    return _create_mssql_source


def create_json_source(database_config):
    def _create_json_source(table, **kwargs):
        return JSONSource(filename=database_config["tables"][table])

    return _create_json_source


def create_database_source(database_config):
    if database_config["type"] == "mssql":
        return create_mssql_source(database_config)
    if database_config["type"] == "json":
        return create_json_source(database_config)


def create_source(source_config, databases) -> Source:
    database_config = databases[source_config["default_database"]]
    database = create_database_source(database_config)
    auth_database_config = databases[source_config["auth_database"]]
    auth_database = create_database_source(auth_database_config)
    return EurofinsSource(
        super_source=database(
            query=source_config["query"], table=source_config["samples"]["table"]
        ),
        auth_source=auth_database(query=source_config["auth_query"]),
        already_done_in=source_config["already_done_in"],
        package_codes=source_config["package_codes"],
        metrics=source_config["metrics"],
        wsdl=source_config.get("wsdl", None),
        schema_version=source_config.get("schema_version", None),
        default_api_key=source_config.get("default_api_key", None),
        default_organization=source_config.get("default_organization", None),
    )


def create_target(target_config) -> Target:
    return ThirtyMHzTarget(**target_config)


def sync_source_to_target(source: Source, target: Target):
    synchronization = Sync(source, target)
    synchronization.start()


def already_done_sync(already_done_in, already_done_out):
    logger.debug("Copying already done")
    with open(already_done_in, "a") as to:
        with open(already_done_out, "r") as fro:
            rows = list(fro.readlines())
            for row in rows:
                Metric.client().set(constants.STATS_APP_SAMPLES_DONE, row)
            if len(rows) > 0:
                to.write("\n")
            to.writelines(rows)


def do_sync(config):
    app_config = config["app"]
    source_config = config[app_config["source"]]
    target_config = config[app_config["target"]]
    databases = config["databases"]
    source = create_source(source_config, databases)
    target = create_target(target_config)
    sync_source_to_target(source, target)
    already_done_sync(
        source_config["already_done_in"], target_config["already_done_out"]
    )


@cli.command()
@click.option("-c", "--config", "config_file")
def sync(config_file):
    """
    This command synchronizes the Eurofins sample data with the 30MHz data.
    """
    logger.add(
        BreadcrumbHandler(level=DEBUG),
        diagnose=True,
        level=DEBUG,
    )
    logger.add(
        EventHandler(level=ERROR),
        diagnose=True,
        level=ERROR,
    )

    logger.add("app{time}.log", retention="1 week", level=DEBUG)
    logger.info("STARTING")
    logger.info("Reading config file")
    config = parse_config(config_file)
    sentry_sdk.init(
        config['sentry']['url'],
        traces_sample_rate=1.0
    )
    logger.info("Initializing statsd client")
    Metric.initialize_client(**config["statsd"])
    Metric.client().incr(constants.STATS_APP_START)
    logger.info("Syncing")
    with Metric.client().timer(constants.STATS_APP_RUNTIME):
        do_sync(config)
    logger.info("______________________________________________________")
