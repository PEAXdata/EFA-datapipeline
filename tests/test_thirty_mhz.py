import tempfile
from random import random, randint
from time import sleep

from efa_30mhz.thirty_mhz import ThirtyMHzTarget, ThirtyMHz
from config import API_KEY, ORGANIZATION


def test_thirty_mhz_target_new_sensor_type():
    #  Writing to the 30mhz target should result in a sensor type being created
    with ThirtyMHzTarget(api_key=API_KEY, organization=ORGANIZATION) as target:
        id = str(random())
        target.write([{
            'id': id,
            'name': f'testdata {id}',
            'data': [
                {
                    'result_value': 417,
                },
            ],
        }])
        assert target.tmz.sensor_type.exists(id=id)


def test_sensor_type_get():
    tmz = ThirtyMHz(api_key=API_KEY, organization=ORGANIZATION)
    l = tmz.sensor_type.list()
    assert len(l) > 0


def test_sensor_type_exists():
    tmz = ThirtyMHz(api_key=API_KEY, organization=ORGANIZATION)
    assert not tmz.sensor_type.exists(id='')
    name = str(random())
    id = str(random())
    c = tmz.sensor_type.create(id=id, name=name, data=[
        {'testdata': 1, 'testdata2': 2}, {'testdata': 3, 'testdata3': 4}
    ])
    print(c)
    assert tmz.sensor_type.exists(id=id)


def test_sensor_type_create():
    tmz = ThirtyMHz(api_key=API_KEY, organization=ORGANIZATION)
    name = str(random())
    id = str(random())
    c = tmz.sensor_type.create(id=id, name=name, data=[
        {'testdata': 1, 'testdata2': 2}, {'testdata': 3, 'testdata3': 4}
    ])
    print(c)
    assert c['name'] == name
    assert c['radioId'] == id


def test_create_i_temp_import_check():
    tmz = ThirtyMHz(api_key=API_KEY, organization=ORGANIZATION)
    id = str(random())
    data = [{
        'data': [{"temp": 13}]}
    ]
    import_check = tmz.import_check.create(id=id, name="test_i_temp",
                                           sensor_type={'typeId': 'i_temp'})
    import_check = tmz.import_check.get(id=id)
    tmz.import_check.ingest(import_check, data)


def test_sensor_type_get_not_exists():
    tmz = ThirtyMHz(api_key=API_KEY, organization=ORGANIZATION)
    assert tmz.sensor_type.get(id=str(random())) is None


def test_infer_type_file():
    temp_file = tempfile.NamedTemporaryFile()
    random_str = str(random())
    temp_file.write(random_str.encode('utf-8'))
    tmz = ThirtyMHz(api_key=API_KEY, organization=ORGANIZATION)
    assert tmz.sensor_type.infer_type(temp_file.file) == 'string'


def test_thirty_mhz_target_file_data():
    # Writing to the 30mhz target should result in a sensor type being created
    f = open('../Test.pdf', 'rb')
    with ThirtyMHzTarget(api_key=API_KEY, organization=ORGANIZATION) as target:
        id = "0.20552255430018695"  # str(random())
        random_result_value = randint(0, 1000)
        target.write([{
            'id': id,
            'name': f'testdata {id}',
            'data': [
                {
                    'result_value': random_result_value,
                    'file': f
                },
            ],
        }])
        sleep(2)
        stats = target.tmz.stats.get(id=id)
        for key in stats['lastRecordedStats'].keys():
            if key.endswith('file'):
                assert 'pdf' in stats['lastRecordedStats'][key]
    f.close()
