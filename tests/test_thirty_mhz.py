from random import random

from loguru import logger

from efa_30mhz.thirty_mhz import ThirtyMHzTarget, ThirtyMHz
from config import API_KEY, ORGANIZATION


def test_thirty_mhz_target():
    with ThirtyMHzTarget(api_key=API_KEY, organization=ORGANIZATION) as target:
        target.write([
            {
                'result_group_id': 6,
                'result_group_description': 'Voederwaarde 2',
                'result_data': [
                    {
                        'result_code': 'VBDS',
                        'result_description': 'DM',
                        'result_value': '417',
                        'result_unit_of_measure_description': 'g/kg product'
                    }
                ]
            }
        ])
        assert False


def test_sensor_type_get():
    tmz = ThirtyMHz(api_key=API_KEY, organization=ORGANIZATION)
    l = tmz.sensor_type.list()
    assert len(l) > 0


def test_sensor_type_exists():
    tmz = ThirtyMHz(api_key=API_KEY, organization=ORGANIZATION)
    assert not tmz.sensor_type.exists(id='')
    desc = str(random())
    id = str(random())
    c = tmz.sensor_type.create(id=id, description=desc, data=[
        {'testdata': 1, 'testdata2': 2}, {'testdata': 3, 'testdata3': 4}
    ])
    assert tmz.sensor_type.exists(id=id)


def test_sensor_type_create():
    tmz = ThirtyMHz(api_key=API_KEY, organization=ORGANIZATION)
    desc = str(random())
    id = str(random())
    c = tmz.sensor_type.create(id=id, description=desc, data=[
        {'testdata': 1, 'testdata2': 2}, {'testdata': 3, 'testdata3': 4}
    ])
    print(c)
    assert c['name'] == desc
    assert c['radioId'] == id
