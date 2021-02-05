import click
from loguru import logger

from efa_30mhz.eurofins import EurofinsSource
from efa_30mhz.sync import Sync
from efa_30mhz.thirty_mhz import ThirtyMHzTarget


@click.group()
@click.option('--debug/--no-debug', default=False)
@logger.catch
def cli(debug):
    logger.info('Debug mode is %s' % ('on' if debug else 'off'))


@cli.command()
def sync():
    """
    This command synchronizes the Eurofins sample data with the 30MHz data.
    """
    logger.info('Syncing')
    with Sync(EurofinsSource, {
        'conn_string': "DRIVER={SQL Server Native Client 11.0};SERVER=localhost;DATABASE=MEA_MAIN;Integrated " \
                       "Security=SSPI;Trusted_Connection=yes; ",
        'table_name': 'sample_data'
    }, ThirtyMHzTarget, {}) as synchronization:
        synchronization.start()
