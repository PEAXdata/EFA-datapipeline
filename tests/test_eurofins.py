from efa_30mhz.eurofins import EurofinsSource

CONNECTION_STRING = "DRIVER={SQL Server Native Client 11.0};SERVER=localhost;DATABASE=MEA_MAIN;Integrated " \
                    "Security=SSPI;Trusted_Connection=yes; "


def test_eurofins_read_all():
    with EurofinsSource(conn_string=CONNECTION_STRING, table_name='sample_data') as source:
        rows = source.read_all()
        assert len(rows) > 0
        assert all([
            len(set(row.keys()).intersection(
                {'order_sample_data_id', 'sample_id', 'sample_code', 'creation_date', 'main_category', 'sub_category',
                 'result_value', 'result_code', 'result_rank', 'result_value_2'})) > 0
            for row in rows
        ])
        # Everything is unique
        tuple_list = [tuple(i.items()) for i in rows]
        assert (len(set(tuple_list)) == len(rows))
