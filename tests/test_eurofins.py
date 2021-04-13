from config import CONNECTION_STRING
from efa_30mhz.eurofins import EurofinsSource


def test_eurofins_read_all_unique():
    def fixup_items(row):
        d = []
        for k, v in row.items():
            data = v
            if isinstance(v, list):
                data = tuple(map(fixup_items, v))
            d.append((k, data))
        return tuple(d)

    with EurofinsSource(conn_string=CONNECTION_STRING, table_name='sample_data') as source:
        rows = source.read_all()
        assert len(rows) > 0
        tuple_list = [tuple(fixup_items(i)) for i in rows]
        assert (len(set(tuple_list)) == len(rows))


def test_eurofins_read_all_standard_form():
    def is_standard_schema(row):
        return set(row.keys()) == {'id', 'name', 'data'}

    with EurofinsSource(conn_string=CONNECTION_STRING, table_name='sample_data') as source:
        rows = source.read_all()
        # Everything is unique
        assert all(is_standard_schema(row) for row in rows)
