from efa_30mhz.eurofins import EurofinsSource, to_thirty_mhz
from efa_30mhz.json import JSONSource

JSON_FILE = 'data/sample_data.json'


def test_eurofins_read_all_unique():
    def fixup_items(row):
        d = []
        for k, v in row.items():
            data = v
            if isinstance(v, list):
                data = tuple(map(fixup_items, v))
            d.append((k, data))
        return tuple(d)

    with EurofinsSource(super_source=JSONSource(filename=JSON_FILE)) as source:
        rows = source.read_all()
        assert len(rows) > 0
        tuple_list = [tuple(fixup_items(i)) for i in rows]
        assert (len(set(tuple_list)) == len(rows))


def test_eurofins_read_all_standard_form():
    def is_standard_schema(row):
        return set(row.keys()) == {'id', 'name', 'data'}

    with EurofinsSource(super_source=JSONSource(filename=JSON_FILE)) as source:
        rows = source.read_all()
        # Everything is unique
        assert all(is_standard_schema(row) for row in rows)


def test_eurofins_to_thirty_mhz():
    sensor_types_real = [
        {
            'id': '210',
            'name': '210',
            'schema': {
                'PH': {
                    'name': 'pH',
                    'type': 'double',
                    'metric': 'ph',
                },
                'EC': {
                    'name': 'EC',
                    'type': 'double',
                    'metric': 'EC-uScm',
                },
                'SO4': {
                    'name': 'S',
                    'type': 'double',
                    'metric': 'parsum',
                },
            }
        },
        {
            'id': '310',
            'name': '310',
            'schema': {
                'PH': {
                    'name': 'pH',
                    'type': 'double',
                    'metric': 'ph',
                },
                'EC': {
                    'name': 'EC',
                    'type': 'double',
                    'metric': 'EC-uScm',
                },
                'SO4': {
                    'name': 'S',
                    'type': 'double',
                    'metric': 'parsum',
                },
            }
        }
    ]
    import_checks_real = [
        {
            'id': '210',
            'name': 'Kasgrond Check',
            'sensor_type': '210'
        },
        {
            'id': '310',
            'name': 'Potgrond Check',
            'sensor_type': '310'
        },
    ]
    ingests_real = [
        {
            'id': '210',
            'data': [{'PH': 6, 'EC': 1, 'NH4': 0, 'K': 0, 'NA': 0, 'CA': 8, 'MG': 1, 'NO3': 3, 'CL': 0, 'SO4': 8,
                      'HCO3': 0,
                      'P': 0, 'FE': 0, 'MN': 0, 'ZN': 0, 'B': 9, 'CU': 0, 'MO': 0, 'SI': 0}]
        },
        {
            'id': '310',
            'data': [{'PH': 5, 'EC': 0, 'NH4': 0, 'K': 1, 'NA': 0, 'CA': 1, 'MG': 0, 'NO3': 1, 'CL': 0, 'SO4': 0,
                      'HCO3': 0,
                      'P': 0, 'FE': 7, 'MN': 1, 'ZN': 2, 'B': 0, 'CU': 0, 'MO': 0, 'SI': 0}]
        },
    ]
    for unit in 'NH4-K-Na-Ca-Mg-NO3-Cl-HCO3-P-Fe-Mn-Zn-B-Cu-Mo-Si'.split('-'):
        sensor_types_real[0]['schema'][unit.upper()] = {
            'name': unit,
            'type': 'double',
            'metric': 'parsum'
        }
        sensor_types_real[1]['schema'][unit.upper()] = sensor_types_real[0]['schema'][unit.upper()]

    with EurofinsSource(super_source=JSONSource(filename=JSON_FILE)) as source:
        rows = source.read_all()
        print(rows)
        sensor_types, import_checks, ingests = to_thirty_mhz(rows)
        assert sensor_types == sensor_types_real
        assert import_checks == import_checks_real
        assert ingests == ingests_real
