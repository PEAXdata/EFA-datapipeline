from random import random

from loguru import logger

from efa_30mhz.thirty_mhz import ThirtyMHzTarget, ThirtyMHz
from config import API_KEY, ORGANIZATION


def test_thirty_mhz_target():
    with ThirtyMHzTarget(api_key=API_KEY, organization=ORGANIZATION) as target:
        target.write([
            {
                'result_group_id': 6,
                'result_group_description': 'Voederwaarde 3',
                'result_data': [
                    {
                        'result_value': 417,
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


def test_create_i_temp_import_check():
    tmz = ThirtyMHz(api_key=API_KEY, organization=ORGANIZATION)
    data = [{
        'result_data': [{"temp": 13}]}
    ]
    if not tmz.import_check.exists(id='test_i_temp'):
        import_check = tmz.import_check.create(id='test_i_temp', description="test_i_temp",
                                               sensor_type={'typeId': 'i_temp'})
    import_check = tmz.import_check.get(id='test_i_temp')
    tmz.import_check.ingest(import_check, data)
