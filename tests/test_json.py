from efa_30mhz.json import JSONSource

JSON_FILE = 'data/sample_data.json'


def test_json_number_of_rows():
    with JSONSource(filename=JSON_FILE) as src:
        data = src.read_all()
        assert len(data) == 4
